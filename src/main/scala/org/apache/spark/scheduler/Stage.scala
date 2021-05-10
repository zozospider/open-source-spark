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

package org.apache.spark.scheduler

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 *
 *
 *
 * Stage (阶段) 是一组 parallel (并行) tasks (任务),
 * 所有这些 tasks (任务) 都计算需要作为 Spark Job 的一部分运行的相同功能, 其中所有 tasks (任务) 都具有相同的 shuffle 依赖关系.
 * 调度程序运行的每个 DAG tasks (任务) 在发生 shuffle 的边界处分为多个 Stage (阶段), 然后 DAGScheduler 以拓扑顺序运行这些 Stage (阶段).
 *
 * 每个 Stage (阶段) 可以是 ShuffleMapStage (在这种情况下其 tasks (任务) 的结果输入到其他 stage(s) (阶段)),
 * 也可以是 ResultStage (在这种情况下其 tasks (任务) 直接计算 Spark 动作) (例如 count(), save(), 等等), 方法是在 RDD 上运行函数.
 * 对于 ShuffleMapStage, 我们还跟踪每个输出 partition (分区) 所在的节点.
 *
 * 每个 Stage (阶段) 还具有一个 firstJobId, 用于标识首先提交该 Stage (阶段) 的 Job.
 * 使用 FIFO 调度时, 这允许首先计算较早 Job 的 Stage (阶段), 或者在发生故障时更快地恢复.
 *
 * 最后, 由于故障恢复, 可以多次尝试重新执行一个 Stage (阶段).
 * 在这种情况下, Stage (阶段) 对象将跟踪多个 StageInfo 对象以传递给侦听器或 Web UI. 最新的将可以通过 latestInfo 访问.
 *
 * @param id 唯一的 Stage (阶段) ID
 *
 * @param rdd 此阶段运行的 RDD: 对于 ShuffleMapStage, 是我们在其上运行 map tasks 的 RDD, 而对于 ResultStage, 是我们对其执行操作的目标 RDD
 *
 * @param numTasks Stage (阶段) 中的 tasks (任务) 总数: ResultStage 尤其可能不需要计算所有 partitions (分区), 例如, 对于 first(), lookup(), 和 take().
 *
 * @param parents 此 Stage (阶段) 所依赖的 stages (阶段) 列表 (通过 shuffle 依赖性).
 *
 * @param firstJobId 此 Stage (阶段) 属于 FIFO 调度的第一个 Job 的 ID.
 *
 * @param callSite 与该 Stage (阶段) 关联的用户程序中的位置: 在目标 RDD 的创建位置, ShuffleMapStage 的位置或在 ResultStage 的动作被调用的位置.
 */
private[scheduler] abstract class Stage(
                                         val id: Int,
                                         val rdd: RDD[_],
                                         val numTasks: Int,
                                         val parents: List[Stage],
                                         val firstJobId: Int,
                                         val callSite: CallSite,
                                         val resourceProfileId: Int)
  extends Logging {

  val numPartitions = rdd.partitions.length

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  /** The ID to use for the next new attempt for this stage. */
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */
  private var _latestInfo: StageInfo =
    StageInfo.fromStage(this, nextAttemptId, resourceProfileId = resourceProfileId)

  /**
   * Set of stage attempt IDs that have failed. We keep track of these failures in order to avoid
   * endless retries if a stage keeps failing.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  val failedAttemptIds = new HashSet[Int]

  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  def makeNewStageAttempt(
                           numPartitionsToCompute: Int,
                           taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences,
      resourceProfileId = resourceProfileId)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  /** 返回缺少的 partition (分区) ID 的序列 (即需要计算). */
  // 分区编号
  def findMissingPartitions(): Seq[Int]

  def isIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }
}
