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

package org.apache.spark.network.util;

/**
 * Selector for which form of low-level IO we should use.
 * NIO is always available, while EPOLL is only available on Linux.
 * AUTO is used to select EPOLL if it's available, or NIO otherwise.
 *
 * 我们应该使用哪种形式的低级别的 IO 的 Selector.
 * NIO 始终可用, 而 EPOLL 仅在 Linux 上可用.
 * AUTO 用于选择 EPOLL (如果可用), 否则用于选择 NIO.
 */
public enum IOMode {
    NIO, EPOLL
}
