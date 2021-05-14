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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 *
 * 超过内存阈值时, 将内存中集合的内容溢出到磁盘上.
 */
private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager) with Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   *
   * @param collection collection to spill to disk
   *
   *
   * 将当前的内存 Collection 溢写到磁盘上, 然后释放内存.
   *
   * @param collection 要溢写到磁盘的 Collection
   */
  // 将 Collection 溢写到磁盘
  protected def spill(collection: C): Unit

  /**
   * Force to spilling the current in-memory collection to disk to release memory,
   * It will be called by TaskMemoryManager when there is not enough memory for the task.
   */
  protected def forceSpill(): Boolean

  // Number of elements read from input since last spill
  protected def elementsRead: Int = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only
  private[this] val initialMemoryThreshold: Long =
  SparkEnv.get.conf.get(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD)

  // Force this collection to spill when there are this many elements in memory
  // For testing only
  private[this] val numElementsForceSpillThreshold: Int =
  SparkEnv.get.conf.get(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  @volatile private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  private[this] var _elementsRead = 0

  // Number of bytes spilled in total
  @volatile private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   *
   *
   * 如果需要, 将当前的内存收集溢出到磁盘上. 尝试在溢出之前获取更多的内存.
   *
   * @param collection 溢写到磁盘的 Collection
   * @param currentMemory Collection 的估计大小 (以字节为单位)
   * @return 如果 `collection` 被溢写到磁盘, 则为 true; 否则为 false
   */
  // 可能要溢写磁盘 (写到临时文件中)
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // 1.
      // 如果 elementsRead % 32 == 0 && 预估 Collection 的内存大小 >= 5M, 则尝试申请新的资源
      // 如果申请的资源不足以容纳数据, 则溢写磁盘

      // Claim up to double our current memory from the shuffle memory pool
      // 要求从 Shuffle 内存池中将我们当前的存储空间增加一倍
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireMemory(amountToRequest)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      // 如果授予我们的内存太少而无法进一步增长 (tryToAcquire 返回的值为 0, 或者我们已经拥有的内存大于 myMemoryThreshold), 请溢出当前 Collection
      shouldSpill = currentMemory >= myMemoryThreshold
    }

    // 2.
    // 如果读取的元素 > 强制溢写磁盘的配置 spark.shuffle.spill.numElementsForceSpillThreshold (默认为 Integer.MAX_VALUE)
    // 则溢写磁盘
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold

    // Actually spill
    // 实际溢写
    if (shouldSpill) {
      _spillCount += 1
      logSpillage(currentMemory)

      // 将 Collection 溢写到磁盘 (写到临时文件中) (具体实现为 ExternalSorter.spill())
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory

      // 释放内存
      releaseMemory()
    }
    shouldSpill
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val isSpilled = forceSpill()
      if (!isSpilled) {
        0L
      } else {
        val freeMemory = myMemoryThreshold - initialMemoryThreshold
        _memoryBytesSpilled += freeMemory
        releaseMemory()
        freeMemory
      }
    } else {
      0L
    }
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   *
   * 将我们的内存释放回 Execution 池, 以便其他任务可以抢占它.
   */
  // 释放内存
  def releaseMemory(): Unit = {
    freeMemory(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long): Unit = {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
