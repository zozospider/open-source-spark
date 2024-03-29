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

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.util.collection.ExternalSorter

// 实现 ShuffleWriter
private[spark] class SortShuffleWriter[K, V, C](
                                                 shuffleBlockResolver: IndexShuffleBlockResolver,
                                                 handle: BaseShuffleHandle[K, V, C],
                                                 mapId: Long,
                                                 context: TaskContext,
                                                 shuffleExecutorComponents: ShuffleExecutorComponents)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output */
  /** 在此 Task (任务) 的输出中写一堆 Records (记录) */
  // 执行 Write
  override def write(records: Iterator[Product2[K, V]]): Unit = {

    // 获取 ExternalSorter (排序器)
    // 如果有 Map 端预聚合, 就传入 aggregator 和 keyOrdering, 否则不传入
    sorter = if (dep.mapSideCombine) {
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      // 在这种情况下, 我们既不传递聚合器也不传递排序器给排序器, 因为我们不在乎键是否在每个分区中排序.
      // 如果正在运行的操作是 sortByKey, 则将在归约方完成.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }

    // 将所有 Records 在内存中进行排序 (可能溢写到磁盘 (写到临时文件中)):
    // 1. 如果有 Map 端预聚合, 则进行聚合, 更新到内存 (可能要溢写磁盘 (对 Collection 进行排序后溢写到磁盘 (写到临时文件中)))
    // 2. 如果没有 Map 端预聚合, 则更新到内存 (可能要溢写磁盘 (对 Collection 进行排序后溢写到磁盘 (写到临时文件中)))
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).


    // 获取 ShuffleMapOutputWriter

    // 写出分区 Map Output 信息
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      dep.shuffleId, mapId, dep.partitioner.numPartitions)

    // 排序合并内存和临时文件, 将排序后的数据写入 mapOutputWriter (ShufflePartitionPairsWriter)
    sorter.writePartitionedMapOutput(dep.shuffleId, mapId, mapOutputWriter)

    // mapOutputWriter (ShufflePartitionPairsWriter) 写入索引文件和数据文件并提交
    val partitionLengths = mapOutputWriter.commitAllPartitions().getPartitionLengths

    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {

  // 1. 没有 Map 端预聚合
  // 2. 下游的分区数量 <= 200 (可配)
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    // 如果需要进行 Map 端聚合, 则无法绕过排序.
    if (dep.mapSideCombine) {
      // 1. 没有 Map 端预聚合

      false
    } else {
      // 2. 下游的分区数量 <= 200 (可配)
      // spark.shuffle.sort.bypassMergeThreshold 默认 200

      val bypassMergeThreshold: Int = conf.get(config.SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
