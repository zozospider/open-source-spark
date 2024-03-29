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

import org.apache.spark.annotation.DeveloperApi

/**
 * An opaque handle to a shuffle, used by a ShuffleManager to pass information about it to tasks.
 *
 * Shuffle 的不透明处理器, 由 ShuffleManager 用于将有关它的信息传递给 Tasks.
 *
 * @param shuffleId ID of the shuffle
 */
// 其实现为:
// - BaseShuffleHandle
// - SerializedShuffleHandle
// - BypassMergeSortShuffleHandle
@DeveloperApi
abstract class ShuffleHandle(val shuffleId: Int) extends Serializable {}
