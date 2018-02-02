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

package org.apache.spark.sql.execution.streaming

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.expressions.CurrentBatchTimestamp
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SparkPlanner, UnaryExecNode}
import org.apache.spark.sql.streaming.OutputMode

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
  * [[QueryExecution]]的一个变体,允许以增量方式执行给定的[[LogicalPlan]]计划,
  * 在每次执行之间可能保存状态。
 */
class IncrementalExecution(
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    val outputMode: OutputMode,
    val checkpointLocation: String,
    val runId: UUID,
    val currentBatchId: Long,
    val offsetSeqMetadata: OffsetSeqMetadata)
  extends QueryExecution(sparkSession, logicalPlan) with Logging {

  // Modified planner with stateful operations.
  //有状态操作的改进的计划者
  override val planner: SparkPlanner = new SparkPlanner(
      sparkSession.sparkContext,
      sparkSession.sessionState.conf,
      sparkSession.sessionState.experimentalMethods) {
    override def strategies: Seq[Strategy] =
      extraPlanningStrategies ++
      sparkSession.sessionState.planner.strategies

    override def extraPlanningStrategies: Seq[Strategy] =
      StatefulAggregationStrategy ::
      FlatMapGroupsWithStateStrategy ::
      StreamingRelationStrategy ::
      StreamingDeduplicationStrategy :: Nil
  }

  /**
   * See [SPARK-18339]
   * Walk the optimized logical plan and replace CurrentBatchTimestamp
   * with the desired literal
    * 步行优化的逻辑计划并用所需的文字替换CurrentBatchTimestamp
   */
  override lazy val optimizedPlan: LogicalPlan = {
    sparkSession.sessionState.optimizer.execute(withCachedData) transformAllExpressions {
      case ts @ CurrentBatchTimestamp(timestamp, _, _) =>
        logInfo(s"Current batch timestamp = $timestamp")
        ts.toLiteral
    }
  }

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
    * 记录查询计划中给定的有状态操作符的当前id,作为“状态”准备工作查询计划
   */
  private val statefulOperatorId = new AtomicInteger(0)

  /** Get the state info of the next stateful operator
    * 获取下一个有状态操作符的状态信息*/
  private def nextStatefulOperationStateInfo(): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(
      checkpointLocation, runId, statefulOperatorId.getAndIncrement(), currentBatchId)
  }

  /** Locates save/restore pairs surrounding aggregation.
    * 找到围绕聚合的保存/恢复对。 */
  val state = new Rule[SparkPlan] {

    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StateStoreSaveExec(keys, None, None, None,
             UnaryExecNode(agg,
               StateStoreRestoreExec(keys2, None, child))) =>
        val aggStateInfo = nextStatefulOperationStateInfo
        StateStoreSaveExec(
          keys,
          Some(aggStateInfo),
          Some(outputMode),
          Some(offsetSeqMetadata.batchWatermarkMs),
          agg.withNewChildren(
            StateStoreRestoreExec(
              keys,
              Some(aggStateInfo),
              child) :: Nil))

      case StreamingDeduplicateExec(keys, child, None, None) =>
        StreamingDeduplicateExec(
          keys,
          child,
          Some(nextStatefulOperationStateInfo),
          Some(offsetSeqMetadata.batchWatermarkMs))

      case m: FlatMapGroupsWithStateExec =>
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermark = Some(offsetSeqMetadata.batchWatermarkMs))
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = state +: super.preparations

  /** No need assert supported, as this check has already been done
    * 没有必要声明支持,因为这项检查已经完成*/
  override def assertSupported(): Unit = { }
}
