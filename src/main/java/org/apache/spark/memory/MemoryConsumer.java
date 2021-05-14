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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 *
 *
 * TaskMemoryManager 的内存消费者支持溢出.
 *
 * 注意: 这仅支持钨内存的 分配 / 溢出.
 */
public abstract class MemoryConsumer {

    protected final TaskMemoryManager taskMemoryManager;
    private final long pageSize;
    private final MemoryMode mode;
    protected long used;

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
        this.taskMemoryManager = taskMemoryManager;
        this.pageSize = pageSize;
        this.mode = mode;
    }

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
        this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
    }

    /**
     * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
     */
    public MemoryMode getMode() {
        return mode;
    }

    /**
     * Returns the size of used memory in bytes.
     */
    public long getUsed() {
        return used;
    }

    /**
     * Force spill during building.
     *
     * 在构建过程中强制溢写.
     */
    // 溢写磁盘
    public void spill() throws IOException {
        // 溢写磁盘
        spill(Long.MAX_VALUE, this);
    }

    /**
     * Spill some data to disk to release memory, which will be called by TaskMemoryManager
     * when there is not enough memory for the task.
     *
     * This should be implemented by subclass.
     *
     * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
     *
     * Note: today, this only frees Tungsten-managed pages.
     *
     * @param size the amount of memory should be released
     * @param trigger the MemoryConsumer that trigger this spilling
     * @return the amount of released memory in bytes
     *
     *
     * 将一些数据溢出到磁盘上以释放内存, 当任务没有足够的内存时, TaskMemoryManager 将调用该数据.
     *
     * 这应该由子类实现.
     *
     * 注意: 为了避免可能的死锁, 请勿从 spill() 调用 acquireMemory().
     *
     * 注意: 今天, 此操作仅释放由钨管理的页面.
     */
    // 溢写磁盘
    // 其实现为 ShuffleExternalSorter.spill()
    public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

    /**
     * Allocates a LongArray of `size`. Note that this method may throw `SparkOutOfMemoryError`
     * if Spark doesn't have enough memory for this allocation, or throw `TooLargePageException`
     * if this `LongArray` is too large to fit in a single page. The caller side should take care of
     * these two exceptions, or make sure the `size` is small enough that won't trigger exceptions.
     *
     * @throws SparkOutOfMemoryError
     * @throws TooLargePageException
     */
    public LongArray allocateArray(long size) {
        long required = size * 8L;
        MemoryBlock page = taskMemoryManager.allocatePage(required, this);
        if (page == null || page.size() < required) {
            throwOom(page, required);
        }
        used += required;
        return new LongArray(page);
    }

    /**
     * Frees a LongArray.
     */
    public void freeArray(LongArray array) {
        freePage(array.memoryBlock());
    }

    /**
     * Allocate a memory block with at least `required` bytes.
     *
     * @throws SparkOutOfMemoryError
     */
    protected MemoryBlock allocatePage(long required) {
        MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
        if (page == null || page.size() < required) {
            throwOom(page, required);
        }
        used += page.size();
        return page;
    }

    /**
     * Free a memory block.
     */
    protected void freePage(MemoryBlock page) {
        used -= page.size();
        taskMemoryManager.freePage(page, this);
    }

    /**
     * Allocates memory of `size`.
     */
    public long acquireMemory(long size) {
        long granted = taskMemoryManager.acquireExecutionMemory(size, this);
        used += granted;
        return granted;
    }

    /**
     * Release N bytes of memory.
     */
    public void freeMemory(long size) {
        taskMemoryManager.releaseExecutionMemory(size, this);
        used -= size;
    }

    private void throwOom(final MemoryBlock page, final long required) {
        long got = 0;
        if (page != null) {
            got = page.size();
            taskMemoryManager.freePage(page, this);
        }
        taskMemoryManager.showMemoryUsage();
        // checkstyle.off: RegexpSinglelineJava
        throw new SparkOutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " +
                got);
        // checkstyle.on: RegexpSinglelineJava
    }
}
