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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.OpenHashSet

/**
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can be spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * Sort-based shuffle has two different write paths for producing its map output files:
 *
 *  - Serialized sorting: used when all three of the following conditions hold:
 *    1. The shuffle dependency specifies no map-side combine.
 *    2. The shuffle serializer supports relocation of serialized values (this is currently
 *       supported by KryoSerializer and Spark SQL's custom serializers).
 *    3. The shuffle produces fewer than or equal to 16777216 output partitions.
 *  - Deserialized sorting: used to handle all other cases.
 *
 * -----------------------
 * Serialized sorting mode
 * -----------------------
 *
 * In the serialized sorting mode, incoming records are serialized as soon as they are passed to the
 * shuffle writer and are buffered in a serialized form during sorting. This write path implements
 * several optimizations:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[ShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on these optimizations, see SPARK-7081.
 *
 *
 *
 * 在基于排序的 Shuffle 中, 传入 Records (记录) 根据其目标分区 ID 进行排序, 然后写入单个 Map Output 文件.
 * Reducers 获取此文件的连续区域, 以读取其 Map Output 的一部分.
 * 如果 Map Output 数据太大而无法容纳在内存中, 则可以将输出的排序子集溢出到磁盘上, 然后将这些磁盘上的文件合并以生成最终的 Output 文件.
 *
 * 基于排序的 Shuffle 具有两种不同的写入路径来生成其 Map Output 文件:
 *
 * - 序列化排序: 在以下三个条件均满足时使用:
 *   1. Shuffle 依赖项未指定 Map 端合并.
 *   2. Shuffle 序列化程序支持重定位序列化的值 (KryoSerializer 和 Spark SQL 的自定义序列化程序当前支持此功能).
 *   3. Shuffle 产生少于或等于 16777216 的 Output 分区.
 * - 反序列化排序: 用于处理所有其他情况.
 *
 * -----------------------
 * 序列化排序模式
 * -----------------------
 *
 * 在序列化排序模式下, 传入的 Records (记录) 一经传递到 Shuffle Writer, 便会进行序列化, 并在排序过程中以序列化的形式进行缓冲.
 * 此写路径实现了几种优化:
 *
 *  - 它的排序对序列化的二进制数据而不是 Java 对象进行操作, 从而减少了内存消耗和 GC 开销.
 *    此优化要求 Record (记录) 序列化程序具有某些属性, 以允许对序列化的 Record (记录) 进行重新排序而无需反序列化.
 *    有关更多详细信息, 请参阅 SPARK-4550, 该优化最初是在该处提出和实施的.
 *
 *  - 它使用专用的高速缓存有效排序器 (ShuffleExternalSorter) 对压缩记录指针和分区 ID 的数组进行排序.
 *    通过在排序数组中每条记录仅使用 8 个字节的空间, 这会将更多的数组放入高速缓存中.
 *
 *  - 溢出合并过程对属于同一分区的序列化记录块进行操作, 并且在合并过程中无需反序列化记录.
 *
 *  - 当溢出压缩编解码器支持压缩数据的级联时, 溢出合并仅将串行化和压缩后的溢出分区进行级联, 以产生最终的 Output 分区.
 *    这允许使用高效的数据复制方法, 例如 NIO 的 `transferTo`, 并且避免了在合并过程中分配解压缩或复制缓冲区的需要.
 *
 * 有关这些优化的更多详细信息, 请参见 SPARK-7081.
 */
// 实现 ShuffleManager
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  import SortShuffleManager._

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)

  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   * 获得 ShuffleHandle 传递给 Tasks.
   */
  // 获得 ShuffleHandle: BypassMergeSortShuffleHandle / SerializedShuffleHandle / BaseShuffleHandle
  override def registerShuffle[K, V, C](
                                         shuffleId: Int,
                                         dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // 1. 不能使用预聚合
      // 2. 如果下游的分区数量 <= 200 (可配)

      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      // 如果分区数量少于 spark.shuffle.sort.bypassMergeThreshold 并且我们不需要 Map 端聚合,
      // 则直接编写 numPartitions文件, 然后在最后将它们连接起来.
      // 这样可以避免执行两次序列化和反序列化以将溢出的文件合并在一起, 而正常的代码路径会发生这种情况.
      // 缺点是一次打开多个文件, 因此分配给缓冲区的内存更多.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])

    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // 1. 序列化规则支持重定位操作 (java 序列化不支持, KRYO 支持)
      // 2. 不能使用预聚合
      // 3. 如果下游的分区数量小于或等于 16777216

      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      // 否则, 请尝试以序列化形式缓冲 Map Outputs, 因为这样做效率更高:
      new SerializedShuffleHandle[K, V](
        shuffleId, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      // 其他情况

      // Otherwise, buffer map outputs in a deserialized form:
      // 否则, 以反序列化形式缓冲区 Map Outputs:
      new BaseShuffleHandle(shuffleId, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from a range of map outputs(startMapIndex to endMapIndex-1, inclusive).
   * If endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length of total map
   * outputs of the shuffle in `getMapSizesByExecutorId`.
   *
   * Called on executors by reduce tasks.
   *
   *
   * 获取 Reader 以了解 Reduce 分区的范围 (包括 startPartition 至 endPartition-1, 包括在内),
   * 以从 Map Outputs 的范围 (从 startMapIndex 到 endMapIndex-1 (包括在内)) 中进行读取.
   * 如果 endMapIndex = Int.MaxValue, 则实际的 endMapIndex 将更改为 getMapSizesByExecutorId 中 Shuffle 的总 Map Outputs 的长度.
   *
   * 通过 Reduce Tasks 来调用 Executors.
   */
  // 获取 Reader (ShuffleReader)
  override def getReader[K, C](
                                handle: ShuffleHandle,
                                startMapIndex: Int,
                                endMapIndex: Int,
                                startPartition: Int,
                                endPartition: Int,
                                context: TaskContext,
                                metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context, metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  /** 获取给定分区的 Writer. 通过 Map Tasks 来调用 Executors. */
  // 获取 Writer (ShuffleWriter: UnsafeShuffleWriter / BypassMergeSortShuffleWriter / SortShuffleWriter)
  override def getWriter[K, V](
                                handle: ShuffleHandle,
                                mapId: Long,
                                context: TaskContext,
                                metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(context.taskAttemptId()) }
    val env = SparkEnv.get

    // 匹配 ShuffleHandle 类型, 确认 ShuffleWriter

    // ShuffleWriter 包含以下实现:
    // - UnsafeShuffleWriter
    // - BypassMergeSortShuffleWriter
    // - SortShuffleWriter
    handle match {

      // ShuffleHandle -> ShuffleWriter
      // SerializedShuffleHandle -> UnsafeShuffleWriter
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)

      // ShuffleHandle -> ShuffleWriter
      // BypassMergeSortShuffleHandle -> BypassMergeSortShuffleWriter
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          bypassMergeSortHandle,
          mapId,
          env.conf,
          metrics,
          shuffleExecutorComponents)

      // ShuffleHandle -> ShuffleWriter
      // BaseShuffleHandle -> SortShuffleWriter
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(
          shuffleBlockResolver, other, mapId, context, shuffleExecutorComponents)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.iterator.foreach { mapTaskId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}


private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   * buffering map outputs in a serialized form. This is an extreme defensive programming measure,
   * since it's extremely unlikely that a single shuffle produces over 16 million output partitions.
   */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * The local property key for continuous shuffle block fetching feature.
   */
  val FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY =
    "__fetch_continuous_blocks_in_batch_enabled"

  /**
   * Helper method for determining whether a shuffle reader should fetch the continuous blocks
   * in batch.
   */
  def canUseBatchFetch(startPartition: Int, endPartition: Int, context: TaskContext): Boolean = {
    val fetchMultiPartitions = endPartition - startPartition > 1
    fetchMultiPartitions &&
      context.getLocalProperty(FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY) == "true"
  }

  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
   *
   * 用于确定 Shuffle 是否应使用优化的序列化 Shuffle 路径或是否应退回到对反序列化对象进行操作的原始路径的辅助方法.
   */
  // 1. 序列化规则支持重定位操作 (java 序列化不支持, KRYO 支持)
  // 2. 不能使用预聚合
  // 3. 如果下游的分区数量小于或等于 16777216
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    if (!dependency.serializer.supportsRelocationOfSerializedObjects) {
      // 1. 序列化规则支持重定位操作 (java 序列化不支持, KRYO 支持)

      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${dependency.serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.mapSideCombine) {
      // 2. 不能使用预聚合

      log.debug(s"Can't use serialized shuffle for shuffle $shufId because we need to do " +
        s"map-side aggregation")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      // 3. 如果下游的分区数量小于或等于 16777216

      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }

  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
      .toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
// 实现 BaseShuffleHandle (实现 ShuffleHandle)
private[spark] class SerializedShuffleHandle[K, V](
                                                    shuffleId: Int,
                                                    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * bypass merge sort shuffle path.
 */
// 实现 BaseShuffleHandle (实现 ShuffleHandle)
private[spark] class BypassMergeSortShuffleHandle[K, V](
                                                         shuffleId: Int,
                                                         dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
