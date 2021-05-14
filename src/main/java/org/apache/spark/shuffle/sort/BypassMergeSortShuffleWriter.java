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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Optional;
import javax.annotation.Nullable;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no map-side combine is specified, and</li>
 *    <li>the number of partitions is less than or equal to
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 *
 *
 * 此类实现基于排序的 Shuffle 的 hash-style Shuffle 后备路径.
 * 此写路径将传入记录写入单独的文件, 每个 Reduce 分区一个文件, 然后将这些 per-partition 文件串联起来形成一个输出文件, 该文件的区域提供给 Reducer.
 * 记录不缓存在内存中. 它以可以通过 `org.apache.spark.shuffle.IndexShuffleBlockResolver` 提供 / 使用 的格式写入输出.
 *
 * 对于具有大量 Reduce 分区的 Shuffle, 此写入路径效率低下, 因为它同时为所有分区打开单独的 Serializers 和文件流.
 * 结果, SortShuffleManager 仅在以下情况下选择此写入路径:
 *
 * - 未指定 Map 端合并
 * - 分区数小于或等于 `spark.shuffle.sort.bypassMergeThreshold`
 *
 * 该代码曾经是 `org.apache.spark.util.collection.ExternalSorter` 的一部分, 但是为了降低代码复杂性, 将其重构为自己的类.
 * 有关详细信息, 请参见 SPARK-7855.
 *
 * 已经有人提出了完全删除该代码路径的建议. 有关详细信息, 请参见 SPARK-6026.
 */
final class BypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

    private final int fileBufferSize;
    private final boolean transferToEnabled;
    private final int numPartitions;
    private final BlockManager blockManager;
    private final Partitioner partitioner;
    private final ShuffleWriteMetricsReporter writeMetrics;
    private final int shuffleId;
    private final long mapId;
    private final Serializer serializer;
    private final ShuffleExecutorComponents shuffleExecutorComponents;

    /** Array of file writers, one for each partition */
    private DiskBlockObjectWriter[] partitionWriters;
    private FileSegment[] partitionWriterSegments;
    @Nullable private MapStatus mapStatus;
    private long[] partitionLengths;

    /**
     * Are we in the process of stopping? Because map tasks can call stop() with success = true
     * and then call stop() with success = false if they get an exception, we want to make sure
     * we don't try deleting files, etc twice.
     */
    private boolean stopping = false;

    BypassMergeSortShuffleWriter(
            BlockManager blockManager,
            BypassMergeSortShuffleHandle<K, V> handle,
            long mapId,
            SparkConf conf,
            ShuffleWriteMetricsReporter writeMetrics,
            ShuffleExecutorComponents shuffleExecutorComponents) {
        // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
        this.fileBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
        this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
        this.blockManager = blockManager;
        final ShuffleDependency<K, V, V> dep = handle.dependency();
        this.mapId = mapId;
        this.shuffleId = dep.shuffleId();
        this.partitioner = dep.partitioner();
        this.numPartitions = partitioner.numPartitions();
        this.writeMetrics = writeMetrics;
        this.serializer = dep.serializer();
        this.shuffleExecutorComponents = shuffleExecutorComponents;
    }

    @Override
    public void write(Iterator<Product2<K, V>> records) throws IOException {
        assert (partitionWriters == null);

        // 新建一个 ShuffleMapOutputWriter
        ShuffleMapOutputWriter mapOutputWriter = shuffleExecutorComponents
                .createMapOutputWriter(shuffleId, mapId, numPartitions);
        try {
            // 如果没有数据的话
            if (!records.hasNext()) {
                // 返回所有分区的写入长度
                partitionLengths = mapOutputWriter.commitAllPartitions().getPartitionLengths();
                // 更新 mapStatus
                mapStatus = MapStatus$.MODULE$.apply(
                        blockManager.shuffleServerId(), partitionLengths, mapId);
                return;
            }
            final SerializerInstance serInstance = serializer.newInstance();
            final long openStartTime = System.nanoTime();
            // 创建和分区数相等的 DiskBlockObjectWriter FileSegment
            partitionWriters = new DiskBlockObjectWriter[numPartitions];
            partitionWriterSegments = new FileSegment[numPartitions];
            // 对于每个分区
            for (int i = 0; i < numPartitions; i++) {
                // 创建一个临时的 block
                final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
                        blockManager.diskBlockManager().createTempShuffleBlock();
                // 获取 temp block 的 file 和 id
                final File file = tempShuffleBlockIdPlusFile._2();
                final BlockId blockId = tempShuffleBlockIdPlusFile._1();
                // 对于每个分区, 创建一个 DiskBlockObjectWriter
                partitionWriters[i] =
                        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
            }
            // Creating the file to write to and creating a disk writer both involve interacting with
            // the disk, and can take a long time in aggregate when we open many files, so should be
            // included in the shuffle write time.
            // 创建要写入的文件和创建磁盘写入器都涉及与磁盘的交互, 并且当我们打开许多文件时可能要花费很长的时间, 因此应将其包括在 Shuffle 写入时间中.
            // 创建文件和写入文件都需要大量时间, 也需要包含在 Shuffle 写入时间里面
            writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

            // 如果有数据的话
            while (records.hasNext()) {
                final Product2<K, V> record = records.next();
                final K key = record._1();
                // 对于每条数据按 key 写入相应分区对应的文件
                partitionWriters[partitioner.getPartition(key)].write(key, record._2());
            }

            for (int i = 0; i < numPartitions; i++) {
                try (DiskBlockObjectWriter writer = partitionWriters[i]) {
                    // 提交
                    partitionWriterSegments[i] = writer.commitAndGet();
                }
            }

            // 将所有分区文件合并成一个文件
            partitionLengths = writePartitionedData(mapOutputWriter);
            // 更新 mapStatus
            mapStatus = MapStatus$.MODULE$.apply(
                    blockManager.shuffleServerId(), partitionLengths, mapId);
        } catch (Exception e) {
            try {
                mapOutputWriter.abort(e);
            } catch (Exception e2) {
                logger.error("Failed to abort the writer after failing to write map output.", e2);
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    @VisibleForTesting
    long[] getPartitionLengths() {
        return partitionLengths;
    }

    /**
     * Concatenate all of the per-partition files into a single combined file.
     * 将所有按分区的文件串联到单个合并文件中.
     *
     * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
     */
    // 将所有分区文件合并成一个文件
    private long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter) throws IOException {
        // Track location of the partition starts in the output file
        if (partitionWriters != null) {
            // 开始时间
            final long writeStartTime = System.nanoTime();
            try {
                for (int i = 0; i < numPartitions; i++) {
                    // 获取每个文件
                    final File file = partitionWriterSegments[i].file();
                    ShufflePartitionWriter writer = mapOutputWriter.getPartitionWriter(i);
                    if (file.exists()) {
                        if (transferToEnabled) {
                            // 采取零拷贝方式

                            // Using WritableByteChannelWrapper to make resource closing consistent between
                            // this implementation and UnsafeShuffleWriter.

                            // 在这里会调用 Utils.copyFileStreamNIO() 方法, 最终调用 FileChannel.transferTo() 方法拷贝文件
                            Optional<WritableByteChannelWrapper> maybeOutputChannel = writer.openChannelWrapper();
                            if (maybeOutputChannel.isPresent()) {
                                writePartitionedDataWithChannel(file, maybeOutputChannel.get());
                            } else {
                                writePartitionedDataWithStream(file, writer);
                            }
                        } else {
                            // 否则采取流的方式拷贝
                            writePartitionedDataWithStream(file, writer);
                        }
                        if (!file.delete()) {
                            logger.error("Unable to delete file for partition {}", i);
                        }
                    }
                }
            } finally {
                writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
            }
            partitionWriters = null;
        }
        return mapOutputWriter.commitAllPartitions().getPartitionLengths();
    }

    private void writePartitionedDataWithChannel(
            File file,
            WritableByteChannelWrapper outputChannel) throws IOException {
        boolean copyThrewException = true;
        try {
            FileInputStream in = new FileInputStream(file);
            try (FileChannel inputChannel = in.getChannel()) {
                Utils.copyFileStreamNIO(
                        inputChannel, outputChannel.channel(), 0L, inputChannel.size());
                copyThrewException = false;
            } finally {
                Closeables.close(in, copyThrewException);
            }
        } finally {
            Closeables.close(outputChannel, copyThrewException);
        }
    }

    private void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
            throws IOException {
        boolean copyThrewException = true;
        FileInputStream in = new FileInputStream(file);
        OutputStream outputStream;
        try {
            outputStream = writer.openStream();
            try {
                Utils.copyStream(in, outputStream, false, false);
                copyThrewException = false;
            } finally {
                Closeables.close(outputStream, copyThrewException);
            }
        } finally {
            Closeables.close(in, copyThrewException);
        }
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        if (stopping) {
            return None$.empty();
        } else {
            stopping = true;
            if (success) {
                if (mapStatus == null) {
                    throw new IllegalStateException("Cannot call stop(true) without having called write()");
                }
                return Option.apply(mapStatus);
            } else {
                // The map task failed, so delete our output data.
                if (partitionWriters != null) {
                    try {
                        for (DiskBlockObjectWriter writer : partitionWriters) {
                            // This method explicitly does _not_ throw exceptions:
                            File file = writer.revertPartialWritesAndClose();
                            if (!file.delete()) {
                                logger.error("Error while deleting file {}", file.getAbsolutePath());
                            }
                        }
                    } finally {
                        partitionWriters = null;
                    }
                }
                return None$.empty();
            }
        }
    }
}
