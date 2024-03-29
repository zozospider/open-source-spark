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

package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, TaskContext}

/**
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 *
 * NOTE:
 * 1. This will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
 * 2. This contains a method ShuffleBlockResolver which interacts with External Shuffle Service
 * when it is enabled. Need to pay attention to that, if implementing a custom ShuffleManager, to
 * make sure the custom ShuffleManager could co-exist with External Shuffle Service.
 *
 *
 * Shuffle 系统的可插拔接口.
 * 基于 spark.shuffle.manager 设置, 在 SparkEnv 中在 Driver 和每个 Executor 上创建一个 ShuffleManager.
 * Driver 向其中注册 Shuffles, Executors (或在 Driver 中的本地运行的任务) 可以要求读取和写入数据.
 *
 * 注意:
 * 1. 这将由 SparkEnv 实例化, 因此其构造函数可以将 SparkConf 和 boolean isDriver 作为参数.
 * 2. 它包含一个 ShuffleBlockResolver 方法, 该方法在启用后会与 External Shuffle Service 交互.
 *    需要注意的是, 如果实现自定义 ShuffleManager, 请确保自定义 ShuffleManager 可以与外部 Shuffle 服务共存.
 */
// 其实现为: SortShuffleManager
private[spark] trait ShuffleManager {

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   *
   * 向 Manager 注册 Shuffle 并获取一个句柄, 以将其传递给 Tasks (任务).
   */
  // 注册 Shuffle
  // 获得 ShuffleHandle: BypassMergeSortShuffleHandle / SerializedShuffleHandle / BaseShuffleHandle
  def registerShuffle[K, V, C](
                                shuffleId: Int,
                                dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** Get a writer for a given partition. Called on executors by map tasks. */
  /** 获取给定分区的 Writer. 通过 Map Tasks 来调用 Executors. */
  // 获取 Writer (ShuffleWriter)
  def getWriter[K, V](
                       handle: ShuffleHandle,
                       mapId: Long,
                       context: TaskContext,
                       metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]


  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive) to
   * read from all map outputs of the shuffle.
   *
   * Called on executors by reduce tasks.
   *
   *
   * 获取 Reader 以了解 Reduce 分区的范围 (包括 startPartition 至 endPartition-1 (包括在内)), 以从 Shuffle 的所有 Map Outputs 中进行读取.
   *
   * 通过 Reduce Tasks 来调用 Executors.
   */
  // 获取 Reader (ShuffleReader)
  final def getReader[K, C](
                             handle: ShuffleHandle,
                             startPartition: Int,
                             endPartition: Int,
                             context: TaskContext,
                             metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReader(handle, 0, Int.MaxValue, startPartition, endPartition, context, metrics)
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
  def getReader[K, C](
                       handle: ShuffleHandle,
                       startMapIndex: Int,
                       endMapIndex: Int,
                       startPartition: Int,
                       endPartition: Int,
                       context: TaskContext,
                       metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return true if the metadata removed successfully, otherwise false.
   */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager. */
  def stop(): Unit
}
