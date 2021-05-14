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

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus

/**
 * The interface for customizing shuffle write process. The driver create a ShuffleWriteProcessor
 * and put it into [[ShuffleDependency]], and executors use it in each ShuffleMapTask.
 *
 * 用于自定义 shuffle 写入过程的接口.
 * Driver 创建一个 ShuffleWriteProcessor 并将其放入 ShuffleDependency 中, Executors 在每个 ShuffleMapTask 中使用它.
 */
// Shuffle Write Processor
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * Create a [[ShuffleWriteMetricsReporter]] from the task context. As the reporter is a
   * per-row operator, here need a careful consideration on performance.
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * The write process for particular partition, it controls the life circle of [[ShuffleWriter]]
   * get from [[ShuffleManager]] and triggers rdd compute, finally return the [[MapStatus]] for
   * this task.
   *
   * 特定分区的写入过程, 它控制从 Shuffle Manager 获取的 Shuffle Writer 的生命周期并触发 RDD 计算, 最后返回此 Task (任务) 的 MapStatus.
   */
  // 执行 Write
  def write(
             rdd: RDD[_],
             dep: ShuffleDependency[_, _, _],
             mapId: Long,
             context: TaskContext,
             partition: Partition): MapStatus = {

    // 获取 Writer (ShuffleWriter)
    var writer: ShuffleWriter[Any, Any] = null
    try {

      // 获取 ShuffleManager - SortShuffleManager
      val manager = SparkEnv.get.shuffleManager

      // 获取 Writer (ShuffleWriter: UnsafeShuffleWriter / BypassMergeSortShuffleWriter / SortShuffleWriter)
      writer = manager.getWriter[Any, Any](
        // dep.shuffleHandle: ShuffleDependency (BypassMergeSortShuffleHandle / SerializedShuffleHandle / BaseShuffleHandle)
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))

      // 执行 Write
      writer.write(
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
