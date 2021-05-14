/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 *
 *
 * 专门用于基于排序的 Shuffle 的外部 Sorter.
 *
 * 传入 Records (记录) 将附加到数据页.
 * 插入所有 Records (记录) 后 (或达到当前线程的随机存储器限制) 时, 将根据其分区 ID (使用 ShuffleInMemorySorter) 对内存中 Records (记录) 进行排序.
 * 然后将排序后的 Records (记录) 写入单个输出文件 (如果溢出, 则写入多个文件).
 * 输出文件的格式与 org.apache.spark.shuffle.sort.SortShuffleWriter 编写的最终输出文件的格式相同:
 * 每个输出分区的记录都写为单个序列化的压缩流, 该流可以通过新的解压缩和反序列化流读取.
 *
 * 与 org.apache.spark.util.collection.ExternalSorter 不同, 此 Sorter 不会合并其溢出文件.
 * 而是在 UnsafeShuffleWriter 中执行此合并, 该合并使用专门的合并过程来避免额外的 序列化/反序列化.
 */
final class ShuffleExternalSorter extends MemoryConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

    @VisibleForTesting
    static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

    private final int numPartitions;
    private final TaskMemoryManager taskMemoryManager;
    private final BlockManager blockManager;
    private final TaskContext taskContext;
    private final ShuffleWriteMetricsReporter writeMetrics;

    /**
     * Force this sorter to spill when there are this many elements in memory.
     */
    private final int numElementsForSpillThreshold;

    /** The buffer size to use when writing spills using DiskBlockObjectWriter */
    private final int fileBufferSizeBytes;

    /** The buffer size to use when writing the sorted records to an on-disk file */
    private final int diskWriteBufferSize;

    /**
     * Memory pages that hold the records being sorted. The pages in this list are freed when
     * spilling, although in principle we could recycle these pages across spills (on the other hand,
     * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
     * itself).
     */
    private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

    private final LinkedList<SpillInfo> spills = new LinkedList<>();

    /** Peak memory used by this sorter so far, in bytes. **/
    private long peakMemoryUsedBytes;

    // These variables are reset after spilling:
    @Nullable private ShuffleInMemorySorter inMemSorter;
    @Nullable private MemoryBlock currentPage = null;
    private long pageCursor = -1;

    ShuffleExternalSorter(
            TaskMemoryManager memoryManager,
            BlockManager blockManager,
            TaskContext taskContext,
            int initialSize,
            int numPartitions,
            SparkConf conf,
            ShuffleWriteMetricsReporter writeMetrics) {
        super(memoryManager,
                (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
                memoryManager.getTungstenMemoryMode());
        this.taskMemoryManager = memoryManager;
        this.blockManager = blockManager;
        this.taskContext = taskContext;
        this.numPartitions = numPartitions;
        // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
        this.fileBufferSizeBytes =
                (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
        this.numElementsForSpillThreshold =
                (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
        this.writeMetrics = writeMetrics;
        this.inMemSorter = new ShuffleInMemorySorter(
                this, initialSize, (boolean) conf.get(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT()));
        this.peakMemoryUsedBytes = getMemoryUsage();
        this.diskWriteBufferSize =
                (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
    }

    /**
     * Sorts the in-memory records and writes the sorted records to an on-disk file.
     * This method does not free the sort data structures.
     *
     * @param isLastFile if true, this indicates that we're writing the final output file and that the
     *                   bytes written should be counted towards shuffle spill metrics rather than
     *                   shuffle write metrics.
     *
     *
     * 对内存中的记录进行排序, 然后将排序后的记录写入磁盘上的文件中.
     * 此方法不会释放排序数据结构.
     *
     * @param isLastFile 如果为 true, 则表明我们正在写入最终输出文件, 并且所写入的字节应计入 Shuffle 溢出指标, 而不是 Shuffle 写入指标.
     */
    // 对内存中的数据进行排序并且将有序记录写到一个磁盘文件中, 这个方法不会释放排序的数据结构
    private void writeSortedFile(boolean isLastFile) {

        // This call performs the actual sort.
        // 返回一个排序好的迭代器
        final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
                inMemSorter.getSortedIterator();

        // If there are no sorted records, so we don't need to create an empty spill file.
        if (!sortedRecords.hasNext()) {
            return;
        }

        final ShuffleWriteMetricsReporter writeMetricsToUse;

        if (isLastFile) {
            // 如果为 true, 则为输出文件, 否则为溢写文件

            // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
            writeMetricsToUse = writeMetrics;
        } else {
            // We're spilling, so bytes written should be counted towards spill rather than write.
            // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
            // them towards shuffle bytes written.
            writeMetricsToUse = new ShuffleWriteMetrics();
        }

        // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
        // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
        // data through a byte array. This array does not need to be large enough to hold a single
        // record;
        // 创建一个字节缓冲数组, 大小为 1M
        final byte[] writeBuffer = new byte[diskWriteBufferSize];

        // Because this output will be read during shuffle, its compression codec must be controlled by
        // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
        // createTempShuffleBlock here; see SPARK-3426 for more details.
        // 创建一个临时的 Shuffle Block
        final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
                blockManager.diskBlockManager().createTempShuffleBlock();
        // 获取文件和 ID
        final File file = spilledFileInfo._2();
        final TempShuffleBlockId blockId = spilledFileInfo._1();
        final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

        // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
        // Our write path doesn't actually use this serializer (since we end up calling the `write()`
        // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
        // around this, we pass a dummy no-op serializer.
        // 不做任何转换的序列化器, 因为需要一个实例来构造 DiskBlockObjectWriter
        final SerializerInstance ser = DummySerializerInstance.INSTANCE;

        int currentPartition = -1;
        final FileSegment committedSegment;
        try (DiskBlockObjectWriter writer =
                     blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse)) {

            final int uaoSize = UnsafeAlignedOffset.getUaoSize();

            // 遍历
            while (sortedRecords.hasNext()) {
                sortedRecords.loadNext();
                final int partition = sortedRecords.packedRecordPointer.getPartitionId();
                assert (partition >= currentPartition);
                if (partition != currentPartition) {
                    // Switch to the new partition
                    if (currentPartition != -1) {
                        // 如果切换到了新的分区, 提交当前分区, 并且记录当前分区大小

                        final FileSegment fileSegment = writer.commitAndGet();
                        spillInfo.partitionLengths[currentPartition] = fileSegment.length();
                    }
                    // 然后切换到下一个分区
                    currentPartition = partition;
                }

                // 获取指针, 通过指针获取页号和偏移量
                final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
                final Object recordPage = taskMemoryManager.getPage(recordPointer);
                final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
                // 获取剩余数据
                int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
                // 跳过数据前面存储的长度
                long recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
                while (dataRemaining > 0) {
                    final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);
                    // 将数据拷贝到缓冲数组中
                    Platform.copyMemory(
                            recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
                    // 从缓冲数组中转入 DiskBlockObjectWriter
                    writer.write(writeBuffer, 0, toTransfer);
                    // 更新位置
                    recordReadPosition += toTransfer;
                    // 更新剩余数据
                    dataRemaining -= toTransfer;
                }
                writer.recordWritten();
            }

            // 提交
            committedSegment = writer.commitAndGet();
        }
        // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
        // then the file might be empty. Note that it might be better to avoid calling
        // writeSortedFile() in that case.
        // 记录溢写文件的列表
        if (currentPartition != -1) {
            spillInfo.partitionLengths[currentPartition] = committedSegment.length();
            spills.add(spillInfo);
        }

        // 如果是溢写文件, 更新溢写的指标
        if (!isLastFile) {  // i.e. this is a spill file
            // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
            // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
            // relies on its `recordWritten()` method being called in order to trigger periodic updates to
            // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
            // counter at a higher-level, then the in-progress metrics for records written and bytes
            // written would get out of sync.
            //
            // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
            // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
            // metrics to the true write metrics here. The reason for performing this copying is so that
            // we can avoid reporting spilled bytes as shuffle write bytes.
            //
            // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
            // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
            // SPARK-3577 tracks the spill time separately.

            // This is guaranteed to be a ShuffleWriteMetrics based on the if check in the beginning
            // of this method.
            writeMetrics.incRecordsWritten(
                    ((ShuffleWriteMetrics)writeMetricsToUse).recordsWritten());
            taskContext.taskMetrics().incDiskBytesSpilled(
                    ((ShuffleWriteMetrics)writeMetricsToUse).bytesWritten());
        }
    }

    /**
     * Sort and spill the current records in response to memory pressure.
     *
     * 根据内存压力对当前 Records (记录) 进行排序和溢出.
     */
    // 溢写磁盘
    // 实现 MemoryConsumer.spill()
    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
            return 0L;
        }

        logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
                Thread.currentThread().getId(),
                Utils.bytesToString(getMemoryUsage()),
                spills.size(),
                spills.size() > 1 ? " times" : " time");

        // 对内存中的数据进行排序并且将有序记录写到一个磁盘文件中, 这个方法不会释放排序的数据结构
        writeSortedFile(false);

        final long spillSize = freeMemory();

        // 重置 ShuffleInMemorySorter
        inMemSorter.reset();
        // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
        // records. Otherwise, if the task is over allocated memory, then without freeing the memory
        // pages, we might not be able to get memory for the pointer array.
        taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
        return spillSize;
    }

    private long getMemoryUsage() {
        long totalPageSize = 0;
        for (MemoryBlock page : allocatedPages) {
            totalPageSize += page.size();
        }
        return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
    }

    private void updatePeakMemoryUsed() {
        long mem = getMemoryUsage();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    private long freeMemory() {
        updatePeakMemoryUsed();
        long memoryFreed = 0;
        for (MemoryBlock block : allocatedPages) {
            memoryFreed += block.size();
            freePage(block);
        }
        allocatedPages.clear();
        currentPage = null;
        pageCursor = 0;
        return memoryFreed;
    }

    /**
     * Force all memory and spill files to be deleted; called by shuffle error-handling code.
     */
    public void cleanupResources() {
        freeMemory();
        if (inMemSorter != null) {
            inMemSorter.free();
            inMemSorter = null;
        }
        for (SpillInfo spill : spills) {
            if (spill.file.exists() && !spill.file.delete()) {
                logger.error("Unable to delete spill file {}", spill.file.getPath());
            }
        }
    }

    /**
     * Checks whether there is enough space to insert an additional record in to the sort pointer
     * array and grows the array if additional space is required. If the required space cannot be
     * obtained, then the in-memory data will be spilled to disk.
     *
     * 检查是否有足够的空间将额外的记录插入到排序指针数组中, 如果需要额外的空间, 则增加数组.
     * 如果无法获得所需的空间, 则内存中的数据将溢出到磁盘上.
     */
    // 检查是否有足够的空间插入额外的记录到排序指针数组中, 如果需要额外的空间对数组进行扩容, 如果空间不够, 内存中的数据将会被溢写到磁盘上
    private void growPointerArrayIfNecessary() throws IOException {
        assert(inMemSorter != null);
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
            // 如果没有空间容纳新的数据

            // 获取当前内存使用量
            long used = inMemSorter.getMemoryUsage();
            LongArray array;
            try {
                // could trigger spilling
                // 分配给缓存原来两倍的容量
                array = allocateArray(used / 8 * 2);

            } catch (TooLargePageException e) {
                // 如果超出了一页的大小, 直接溢写, 溢写方法见后面
                // 一页的大小为 128M, 在 PackedRecordPointer 类中

                // The pointer array is too big to fix in a single page, spill.
                // 指针数组太大而无法固定在单个页面中, 溢出.

                // 溢写磁盘
                // 其实现为 ShuffleExternalSorter.spill()
                spill();

                return;
            } catch (SparkOutOfMemoryError e) {
                // should have trigger spilling
                if (!inMemSorter.hasSpaceForAnotherRecord()) {
                    logger.error("Unable to grow the pointer array");
                    throw e;
                }
                return;
            }
            // check if spilling is triggered or not
            if (inMemSorter.hasSpaceForAnotherRecord()) {
                // 如果有了剩余空间, 则表明没必要扩容, 释放分配的空间

                freeArray(array);
            } else {
                // 否则把原来的数组复制到新的数组

                inMemSorter.expandPointerArray(array);
            }
        }
    }

    /**
     * Allocates more memory in order to insert an additional record. This will request additional
     * memory from the memory manager and spill if the requested memory can not be obtained.
     *
     * @param required the required space in the data page, in bytes, including space for storing
     *                      the record size. This must be less than or equal to the page size (records
     *                      that exceed the page size are handled via a different code path which uses
     *                      special overflow pages).
     */
    private void acquireNewPageIfNecessary(int required) {
        if (currentPage == null ||
                pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
            // TODO: try to find space in previous pages
            currentPage = allocatePage(required);
            pageCursor = currentPage.getBaseOffset();
            allocatedPages.add(currentPage);
        }
    }

    /**
     * Write a record to the shuffle sorter.
     * 将 Record (记录) 写入 Shuffle Sorter.
     */
    // 将序列化后的数据插入 ShuffleExternalSorter 处理
    // 在这里对于数据的缓存和溢写不借助于其他高级数据结构, 而是直接操作内存空间
    public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
            throws IOException {

        // for tests
        assert(inMemSorter != null);
        // 如果数据条数超过溢写阈值, 直接溢写磁盘
        if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
            logger.info("Spilling data because number of spilledRecords crossed the threshold " +
                    numElementsForSpillThreshold);
            spill();
        }

        // 检查是否有足够的空间插入额外的记录到排序指针数组中, 如果需要额外的空间对数组进行扩容, 如果空间不够, 内存中的数据将会被溢写到磁盘上
        growPointerArrayIfNecessary();

        final int uaoSize = UnsafeAlignedOffset.getUaoSize();
        // Need 4 or 8 bytes to store the record length.
        // 需要额外的 4 或 8 个字节存储数据长度
        final int required = length + uaoSize;
        // 如果需要更多的内存, 会向 TaskMemoryManager 申请新的 page
        acquireNewPageIfNecessary(required);

        assert(currentPage != null);
        final Object base = currentPage.getBaseObject();

        // 通过给定的内存页和偏移量, 将当前数据的逻辑地址编码成一个 Long 型
        final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
        // 写长度值
        UnsafeAlignedOffset.putSize(base, pageCursor, length);
        // 移动指针
        pageCursor += uaoSize;
        // 写数据
        Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
        // 移动指针
        pageCursor += length;
        // 将编码的逻辑地址和分区 ID 传给 ShuffleInMemorySorter 进行排序
        inMemSorter.insertRecord(recordAddress, partitionId);
    }

    /**
     * Close the sorter, causing any buffered data to be sorted and written out to disk.
     *
     * @return metadata for the spill files written by this sorter. If no records were ever inserted
     *         into this sorter, then this will return an empty array.
     */
    public SpillInfo[] closeAndGetSpills() throws IOException {
        if (inMemSorter != null) {
            // Do not count the final file towards the spill count.
            writeSortedFile(true);
            freeMemory();
            inMemSorter.free();
            inMemSorter = null;
        }
        return spills.toArray(new SpillInfo[spills.size()]);
    }

}
