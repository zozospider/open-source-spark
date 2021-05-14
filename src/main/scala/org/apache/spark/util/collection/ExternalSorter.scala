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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer._
import org.apache.spark.shuffle.ShufflePartitionPairsWriter
import org.apache.spark.shuffle.api.{ShuffleMapOutputWriter, ShufflePartitionWriter}
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter, ShuffleBlockId}
import org.apache.spark.util.{Utils => TryUtils}

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 *
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 * @param serializer serializer to use when spilling to disk
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 *
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.
 *
 * 2. Call insertAll() with a set of records.
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *
 * At a high level, this class works internally as follows:
 *
 *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *    we want to combine by key, or a PartitionedPairBuffer if we don't.
 *    Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *    To avoid calling the partitioner multiple times with each key, we store the partition ID
 *    alongside each record.
 *
 *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *    by partition ID and possibly second by key or by hash code of the key, if we want to do
 *    aggregation. For each file, we track how many objects were in each partition in memory, so we
 *    don't have to write out the partition ID for every element.
 *
 *  - When the user requests an iterator or file output, the spilled files are merged, along with
 *    any remaining in-memory data, using the same sort order defined above (unless both sorting
 *    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *    from the ordering parameter, or read the keys with the same hash code and compare them with
 *    each other for equality to merge values.
 *
 *  - Users are expected to call stop() at the end to delete all the intermediate files.
 *
 *
 *
 * 排序并可能合并多个类型为 (K, V) 的 key-value 对以生成类型为 (K,C) 的 key-combiner 对.
 * 使用 Partitioner 首先将键分组到 Partitions 中, 然后使用自定义 Comparator (比较器) 对每个 Partition 中的键进行有选择的排序.
 * 可以输出每个 Partition 具有不同字节范围的单个分区文件, 适用于 Shuffle 读取.
 *
 * 如果禁用组合, 则类型 C 必须等于 V -- 我们将在最后投射对象.
 *
 * 注意: 尽管 ExternalSorter 是一个相当通用的排序器, 但其某些配置与其在基于排序的 Shuffle 中的使用有关 (例如, 其块压缩由 `spark.shuffle.compress` 控制).
 * 如果在其他我们希望使用其他配置设置的非 Shuffle 上下文中使用了 ExternalSorter, 则可能需要重新访问此设置.
 *
 * @param aggregator 具有合并功能的可选聚合器, 用于合并数据
 * @param partitioner 可选的分区程序: 如果给定, 则按分区 ID 排序, 然后按 key 排序
 * @param ordering 可选的排序顺序, 对每个分区内的 key 进行排序; 应该是总排序
 * @param serializer 溢出到磁盘时要使用的序列化器
 *
 * 请注意, 如果给出了 Ordering, 我们将始终使用它进行排序, 因此只有在您确实希望对输出 keys 进行排序时才提供它.
 * 例如, 在没有 Map 端合并的 Map Task 中, 您可能希望传递 None 作为顺序, 以避免额外的排序.
 * 另一方面, 如果您确实想进行合并, 那么拥有排序会比没有排序更有效率.
 *
 * 用户通过以下方式与此类进行交互:
 * 1. 实例化一个 ExternalSorter.
 * 2. 使用一组 Records (记录) 调用 insertAll().
 * 3. 请求 iterator() 返回遍历 排序/聚合 的 Records (记录).
 *    - 或者 -
 *    调用 writePartitionedFile() 创建一个包含 排序 / 聚合 输出的文件, 该输出可在 Spark 的排序 Shuffle 中使用.
 *
 * 在较高的层次上, 该类的内部工作如下:
 *
 * - 我们重复填充内存数据的缓冲区, 如果要通过 key 组合, 则使用 PartitionedAppendOnlyMap, 如果不想, 则使用 PartitionedPairBuffer.
 *   在这些缓冲区中, 我们按分区 ID 对元素进行排序, 然后还可能按键对元素进行排序.
 *   为了避免用每个键多次调用分区程序, 我们将分区 ID 与每个 Record (记录) 一起存储.
 *
 * - 当每个缓冲区达到内存限制时, 我们会将其溢出到文件中.
 *   如果要进行聚合, 则此文件首先按分区 ID 排序, 然后可能按 key 或 key 的 hash code 排序.
 *   对于每个文件, 我们跟踪内存中每个分区中有多少个对象, 因此我们不必为每个元素写出分区 ID.
 *
 * - 当用户请求 iterator 或文件输出时, 溢出的文件将与所有剩余的内存中数据一起使用上面定义的相同排序顺序合并 (除非同时禁用了排序和聚合).
 *   如果需要按键进行聚合, 则可以使用来自排序参数的总排序, 或者使用相同的 hash code 读取 keys, 然后将它们相互比较以等同于合并值.
 *
 * - 希望用户在最后调用 stop() 来删除所有中间文件.
 */
private[spark] class ExternalSorter[K, V, C](
                                              context: TaskContext,
                                              aggregator: Option[Aggregator[K, V, C]] = None,
                                              partitioner: Option[Partitioner] = None,
                                              ordering: Option[Ordering[K]] = None,
                                              serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[WritablePartitionedPairCollection[K, C]](context.taskMemoryManager())
    with Logging {

  private val conf = SparkEnv.get.conf

  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  private val shouldPartition = numPartitions > 1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.get(config.SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.get(config.SHUFFLE_SPILL_BATCH_SIZE)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  // 在溢出之前用于存储内存对象的数据结构.
  // 根据是否设置了 Aggregator, 我们可以将对象放入 AppendOnlyMap 中以将它们组合在一起, 或者将它们存储在数组缓冲区中.
  @volatile private var map = new PartitionedAppendOnlyMap[K, C]
  @volatile private var buffer = new PartitionedPairBuffer[K, C]

  // Total spilling statistics
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  @volatile private var isShuffleSort: Boolean = true
  private val forceSpillFiles = new ArrayBuffer[SpilledFile]
  @volatile private var readingIterator: SpillableIterator = null

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  private val keyComparator: Comparator[K] = ordering.getOrElse((a: K, b: K) => {
    val h1 = if (a == null) 0 else a.hashCode()
    val h2 = if (b == null) 0 else b.hashCode()
    if (h1 < h2) -1 else if (h1 == h2) 0 else 1
  })

  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private[this] case class SpilledFile(
                                        file: File,
                                        blockId: BlockId,
                                        serializerBatchSizes: Array[Long],
                                        elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * Exposed for testing.
   */
  private[spark] def numSpills: Int = spills.size

  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    if (shouldCombine) {
      // 有预聚合

      // Combine values in-memory first using our AppendOnlyMap
      // 首先使用我们的 AppendOnlyMap 在内存中合并 values
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null

      // 更新 value
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (records.hasNext) {
        addElementsRead()

        // 更新聚合后的数据到内存 (使用 Map 结构)
        kv = records.next()
        map.changeValue((getPartition(kv._1), kv._1), update)

        // 可能要溢写磁盘
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // 没有预聚合

      // Stick values into our buffer
      // 将 values 放入我们的 buffer
      while (records.hasNext) {
        addElementsRead()

        // 直接放入 buffer (使用 Buffer 结构)
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])

        // 可能要溢写磁盘
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *
   * 如果需要, 将当前的内存收集溢出到磁盘上.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   *
   *
   * 如果需要, 将当前的内存收集溢出到磁盘上.
   *
   * @param usingMap 我们是使用 Map 还是 Buffer 作为当前的内存中集合
   */
  // 可能要溢写磁盘
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
      // Map 结构

      // 估计 Collection 的当前大小 (以字节为单位). O(1) time.
      estimatedSize = map.estimateSize()
      if (maybeSpill(map, estimatedSize)) {
        // 重新创建 Map
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      // Buffer 结构

      // 估计 Collection 的当前大小 (以字节为单位). O(1) time.
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        // 重新创建 Buffer
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    val inMemoryIterator = collection.destructiveSortedWritablePartitionedIterator(comparator)
    val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
    spills += spillFile
  }

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  override protected[this] def forceSpill(): Boolean = {
    if (isShuffleSort) {
      false
    } else {
      assert(readingIterator != null)
      val isSpilled = readingIterator.spill()
      if (isSpilled) {
        map = null
        buffer = null
      }
      isSpilled
    }
  }

  /**
   * Spill contents of in-memory iterator to a temporary file on disk.
   */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: WritablePartitionedIterator)
  : SpilledFile = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      _diskBytesSpilled += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        val partitionId = inMemoryIterator.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        inMemoryIterator.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition)
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))
    val inMemBuffered = inMemory.buffered
    (0 until numPartitions).iterator.map { p =>
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (aggregator.isDefined) {
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
  : Iterator[Product2[K, C]] = {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    // Use the reverse order (compare(y,x)) because PriorityQueue dequeues the max
    val heap = new mutable.PriorityQueue[Iter]()(
      (x: Iter, y: Iter) => comparator.compare(y.head._1, x.head._1))
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = heap.nonEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  private def mergeWithAggregation(
                                    iterators: Seq[Iterator[Product2[K, C]]],
                                    mergeCombiners: (C, C) => C,
                                    comparator: Comparator[K],
                                    totalOrder: Boolean)
  : Iterator[Product2[K, C]] = {
    if (!totalOrder) {
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      val it = new Iterator[Iterator[Product2[K, C]]] {
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
      }
      it.flatten
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream()  // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** Construct a stream that only reads from the next batch */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)
        batchId += 1

        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

        val wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream)
        serInstance.deserializeStream(wrappedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     */
    private def skipToNextPartition(): Unit = {
      while (partitionId < numPartitions &&
        indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        return null
      }
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      indexInBatch += 1
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      indexInPartition += 1
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0

    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup(): Unit = {
      batchId = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      if (ds != null) {
        ds.close()
      }
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Returns a destructive iterator for iterating over the entries of this map.
   * If this iterator is forced spill to disk to release memory when there is not enough memory,
   * it returns pairs from an on-disk map.
   */
  def destructiveIterator(memoryIterator: Iterator[((Int, K), C)]): Iterator[((Int, K), C)] = {
    if (isShuffleSort) {
      memoryIterator
    } else {
      readingIterator = new SpillableIterator(memoryIterator)
      readingIterator
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (ordering.isEmpty) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(None)))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(destructiveIterator(
          collection.partitionedDestructiveSortedIterator(Some(keyComparator))))
      }
    } else {
      // Merge spilled and in-memory data
      merge(spills.toSeq, destructiveIterator(
        collection.partitionedDestructiveSortedIterator(comparator)))
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   */
  def iterator: Iterator[Product2[K, C]] = {
    isShuffleSort = false
    partitionedIterator.flatMap(pair => pair._2)
  }

  /**
   * TODO(SPARK-28764): remove this, as this is only used by UnsafeRowSerializerSuite in the SQL
   * project. We should figure out an alternative way to test that so that we can remove this
   * otherwise unused code path.
   */
  def writePartitionedFile(
                            blockId: BlockId,
                            outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)
    val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
      context.taskMetrics().shuffleWriteMetrics)

    if (spills.isEmpty) {
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        val segment = writer.commitAndGet()
        lengths(partitionId) = segment.length
      }
    } else {
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          val segment = writer.commitAndGet()
          lengths(id) = segment.length
        }
      }
    }

    writer.close()
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }

  /**
   * Write all the data added into this ExternalSorter into a map output writer that pushes bytes
   * to some arbitrary backing store. This is called by the SortShuffleWriter.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   *
   *
   * 将添加到此 ExternalSorter 中的所有数据写入 Map Output writer, 该输出将字节推送到某个任意后备存储.
   * 这由 SortShuffleWriter 调用.
   *
   * @return 文件每个 Partition (分区) 的长度 (以字节为单位) 的长度数组 (由 Map Output Tracker 使用)
   */
  def writePartitionedMapOutput(
                                 shuffleId: Int,
                                 mapId: Long,
                                 mapOutputWriter: ShuffleMapOutputWriter): Unit = {
    var nextPartitionId = 0
    if (spills.isEmpty) {
      // Case where we only have in-memory data
      // 只有内存数据的情况
      val collection = if (aggregator.isDefined) map else buffer
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext()) {
        val partitionId = it.nextPartition()
        var partitionWriter: ShufflePartitionWriter = null
        var partitionPairsWriter: ShufflePartitionPairsWriter = null
        TryUtils.tryWithSafeFinally {
          partitionWriter = mapOutputWriter.getPartitionWriter(partitionId)
          val blockId = ShuffleBlockId(shuffleId, mapId, partitionId)
          partitionPairsWriter = new ShufflePartitionPairsWriter(
            partitionWriter,
            serializerManager,
            serInstance,
            blockId,
            context.taskMetrics().shuffleWriteMetrics)
          while (it.hasNext && it.nextPartition() == partitionId) {
            it.writeNext(partitionPairsWriter)
          }
        } {
          if (partitionPairsWriter != null) {
            partitionPairsWriter.close()
          }
        }
        nextPartitionId = partitionId + 1
      }
    } else {
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      // 我们必须执行 merge-sort (合并排序): 通过 Partition (分区) 获取 Iterator, 然后直接编写所有内容.
      for ((id, elements) <- this.partitionedIterator) {
        val blockId = ShuffleBlockId(shuffleId, mapId, id)
        var partitionWriter: ShufflePartitionWriter = null
        var partitionPairsWriter: ShufflePartitionPairsWriter = null
        TryUtils.tryWithSafeFinally {
          partitionWriter = mapOutputWriter.getPartitionWriter(id)
          partitionPairsWriter = new ShufflePartitionPairsWriter(
            partitionWriter,
            serializerManager,
            serInstance,
            blockId,
            context.taskMetrics().shuffleWriteMetrics)
          if (elements.hasNext) {
            for (elem <- elements) {
              partitionPairsWriter.write(elem._1, elem._2)
            }
          }
        } {
          if (partitionPairsWriter != null) {
            partitionPairsWriter.close()
          }
        }
        nextPartitionId = id + 1
      }
    }

    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)
  }

  def stop(): Unit = {
    spills.foreach(s => s.file.delete())
    spills.clear()
    forceSpillFiles.foreach(s => s.file.delete())
    forceSpillFiles.clear()
    if (map != null || buffer != null || readingIterator != null) {
      map = null // So that the memory can be garbage-collected
      buffer = null // So that the memory can be garbage-collected
      readingIterator = null // So that the memory can be garbage-collected
      releaseMemory()
    }
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
  : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }

  private[this] class SpillableIterator(var upstream: Iterator[((Int, K), C)])
    extends Iterator[((Int, K), C)] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[((Int, K), C)] = null

    private var cur: ((Int, K), C) = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        val inMemoryIterator = new WritablePartitionedIterator {
          private[this] var cur = if (upstream.hasNext) upstream.next() else null

          def writeNext(writer: PairsWriter): Unit = {
            writer.write(cur._1._2, cur._2)
            cur = if (upstream.hasNext) upstream.next() else null
          }

          def hasNext(): Boolean = cur != null

          def nextPartition(): Int = cur._1._1
        }
        logInfo(s"Task ${TaskContext.get().taskAttemptId} force spilling in-memory map to disk " +
          s"and it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        val spillFile = spillMemoryIteratorToDisk(inMemoryIterator)
        forceSpillFiles += spillFile
        val spillReader = new SpillReader(spillFile)
        nextUpstream = (0 until numPartitions).iterator.flatMap { p =>
          val iterator = spillReader.readNextPartition()
          iterator.map(cur => ((p, cur._1), cur._2))
        }
        hasSpilled = true
        true
      }
    }

    def readNext(): ((Int, K), C) = SPILL_LOCK.synchronized {
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): ((Int, K), C) = {
      val r = cur
      cur = readNext()
      r
    }
  }
}
