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

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent
import org.apache.spark.util.Clock

/**
 * Responsible for continually reporting statistics about the amount of data processed as well
 * as latency for a streaming query.  This trait is designed to be mixed into the
 * [[StreamExecution]], who is responsible for calling `startTrigger` and `finishTrigger`
 * at the appropriate times. Additionally, the status can updated with `updateStatusMessage` to
 * allow reporting on the streams current state (i.e. "Fetching more data").
  * 负责持续报告已处理数据量的统计信息以及流式查询的延迟时间,这个特征被设计成混合到[[StreamExecution]]中,
  * 负责在适当的时候调用`startTrigger`和`finishTrigger`,
  * 另外,状态可以用`updateStatusMessage`更新以允许报告流当前状态(即“获取更多数据”)
 */
trait ProgressReporter extends Logging {

  case class ExecutionStats(
    inputRows: Map[Source, Long],
    stateOperators: Seq[StateOperatorProgress],
    eventTimeStats: Map[String, String])

  // Internal state of the stream, required for computing metrics.
  //计算指标所需的流的内部状态
  protected def id: UUID
  protected def runId: UUID
  protected def name: String
  protected def triggerClock: Clock
  protected def logicalPlan: LogicalPlan
  protected def lastExecution: QueryExecution
  protected def newData: Map[Source, DataFrame]
  protected def availableOffsets: StreamProgress
  protected def committedOffsets: StreamProgress
  protected def sources: Seq[Source]
  protected def sink: Sink
  protected def offsetSeqMetadata: OffsetSeqMetadata
  protected def currentBatchId: Long
  protected def sparkSession: SparkSession
  protected def postEvent(event: StreamingQueryListener.Event): Unit

  // Local timestamps and counters.
  //本地时间戳和计数器
  private var currentTriggerStartTimestamp = -1L
  private var currentTriggerEndTimestamp = -1L
  // TODO: Restore this from the checkpoint when possible.
  private var lastTriggerStartTimestamp = -1L
  private val currentDurationsMs = new mutable.HashMap[String, Long]()

  /** Flag that signals whether any error with input metrics have already been logged
    * 表示输入指标是否有任何错误已被记录的标志*/
  private var metricWarningLogged: Boolean = false

  /** Holds the most recent query progress updates.  Accesses must lock on the queue itself.
    * 保留最近的查询进度更新,访问必须锁定队列本身 */
  private val progressBuffer = new mutable.Queue[StreamingQueryProgress]()

  private val noDataProgressEventInterval =
    sparkSession.sessionState.conf.streamingNoDataProgressEventInterval

  // The timestamp we report an event that has no input data
  //我们报告没有输入数据的事件的时间戳
  private var lastNoDataProgressEventTime = Long.MinValue

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(DateTimeUtils.getTimeZone("UTC"))

  @volatile
  protected var currentStatus: StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = "Initializing StreamExecution",
      isDataAvailable = false,
      isTriggerActive = false)
  }

  /** Returns the current status of the query.
    * 返回查询的当前状态*/
  def status: StreamingQueryStatus = currentStatus

  /** Returns an array containing the most recent query progress updates.
    * 返回包含最近查询进度更新的数组*/
  def recentProgress: Array[StreamingQueryProgress] = progressBuffer.synchronized {
    progressBuffer.toArray
  }

  /** Returns the most recent query progress update or null if there were no progress updates.
    * 返回最近的查询进度更新;如果没有进度更新,则返回null */
  def lastProgress: StreamingQueryProgress = progressBuffer.synchronized {
    progressBuffer.lastOption.orNull
  }

  /** Begins recording statistics about query progress for a given trigger.
    * 开始记录有关给定触发器的查询进度的统计信息 */
  protected def startTrigger(): Unit = {
    logDebug("Starting Trigger Calculation")
    lastTriggerStartTimestamp = currentTriggerStartTimestamp
    currentTriggerStartTimestamp = triggerClock.getTimeMillis()
    currentStatus = currentStatus.copy(isTriggerActive = true)
    currentDurationsMs.clear()
  }

  private def updateProgress(newProgress: StreamingQueryProgress): Unit = {
    progressBuffer.synchronized {
      progressBuffer += newProgress
      while (progressBuffer.length >= sparkSession.sqlContext.conf.streamingProgressRetention) {
        progressBuffer.dequeue()
      }
    }
    postEvent(new QueryProgressEvent(newProgress))
    logInfo(s"Streaming query made progress: $newProgress")
  }

  /** Finalizes the query progress and adds it to list of recent status updates.
    * 完成查询进度并将其添加到最近的状态更新列表中*/
  protected def finishTrigger(hasNewData: Boolean): Unit = {
    currentTriggerEndTimestamp = triggerClock.getTimeMillis()

    val executionStats = extractExecutionStats(hasNewData)
    val processingTimeSec =
      (currentTriggerEndTimestamp - currentTriggerStartTimestamp).toDouble / 1000

    val inputTimeSec = if (lastTriggerStartTimestamp >= 0) {
      (currentTriggerStartTimestamp - lastTriggerStartTimestamp).toDouble / 1000
    } else {
      Double.NaN
    }
    logDebug(s"Execution stats: $executionStats")

    val sourceProgress = sources.map { source =>
      val numRecords = executionStats.inputRows.getOrElse(source, 0L)
      new SourceProgress(
        description = source.toString,
        startOffset = committedOffsets.get(source).map(_.json).orNull,
        endOffset = availableOffsets.get(source).map(_.json).orNull,
        numInputRows = numRecords,
        inputRowsPerSecond = numRecords / inputTimeSec,
        processedRowsPerSecond = numRecords / processingTimeSec
      )
    }
    val sinkProgress = new SinkProgress(sink.toString)

    val newProgress = new StreamingQueryProgress(
      id = id,
      runId = runId,
      name = name,
      timestamp = formatTimestamp(currentTriggerStartTimestamp),
      batchId = currentBatchId,
      durationMs = new java.util.HashMap(currentDurationsMs.toMap.mapValues(long2Long).asJava),
      eventTime = new java.util.HashMap(executionStats.eventTimeStats.asJava),
      stateOperators = executionStats.stateOperators.toArray,
      sources = sourceProgress.toArray,
      sink = sinkProgress)

    if (hasNewData) {
      // Reset noDataEventTimestamp if we processed any data
      lastNoDataProgressEventTime = Long.MinValue
      updateProgress(newProgress)
    } else {
      val now = triggerClock.getTimeMillis()
      if (now - noDataProgressEventInterval >= lastNoDataProgressEventTime) {
        lastNoDataProgressEventTime = now
        updateProgress(newProgress)
      }
    }

    currentStatus = currentStatus.copy(isTriggerActive = false)
  }

  /** Extract statistics about stateful operators from the executed query plan.
    * 从执行的查询计划中提取关于有状态操作符的统计信息*/
  private def extractStateOperatorMetrics(hasNewData: Boolean): Seq[StateOperatorProgress] = {
    if (lastExecution == null) return Nil
    // lastExecution could belong to one of the previous triggers if `!hasNewData`.
    // Walking the plan again should be inexpensive.
    //如果`！hasNewData`,lastExecution可能属于之前的触发器之一,再次行动计划应该是便宜的。
    lastExecution.executedPlan.collect {
      case p if p.isInstanceOf[StateStoreWriter] =>
        val progress = p.asInstanceOf[StateStoreWriter].getProgress()
        if (hasNewData) progress else progress.copy(newNumRowsUpdated = 0)
    }
  }

  /** Extracts statistics from the most recent query execution.
    * 从最近的查询执行中提取统计信息*/
  private def extractExecutionStats(hasNewData: Boolean): ExecutionStats = {
    val hasEventTime = logicalPlan.collect { case e: EventTimeWatermark => e }.nonEmpty
    val watermarkTimestamp =
      if (hasEventTime) Map("watermark" -> formatTimestamp(offsetSeqMetadata.batchWatermarkMs))
      else Map.empty[String, String]

    // SPARK-19378: Still report metrics even though no data was processed while reporting progress.
    //即使在报告进度时没有处理任何数据,仍然会报告指标
    val stateOperators = extractStateOperatorMetrics(hasNewData)

    if (!hasNewData) {
      return ExecutionStats(Map.empty, stateOperators, watermarkTimestamp)
    }

    // We want to associate execution plan leaves to sources that generate them, so that we match
    // the their metrics (e.g. numOutputRows) to the sources. To do this we do the following.
    // Consider the translation from the streaming logical plan to the final executed plan.
    //我们希望将执行计划的叶子与生成它们的源相关联,
    // 以便我们将它们的度量（例如numOutputRows）与源相匹配,要做到这一点,我们做到以下几点。
    // 考虑从流逻辑计划到最终执行计划的翻译。
    //
    //  streaming logical plan (with sources) <==> trigger's logical plan <==> executed plan
    //流式逻辑计划（带有源代码）<==>触发器的逻辑计划<==>执行计划
    //
    // 1. We keep track of streaming sources associated with each leaf in the trigger's logical plan
    // 1.我们跟踪触发器逻辑计划中与每个叶子相关的流源
    //    - Each logical plan leaf will be associated with a single streaming source.
    //      每个逻辑计划叶子将与一个单一的流源相关联。
    //    - There can be multiple logical plan leaves associated with a streaming source.
    //      可以有多个与流源相关联的逻辑计划页面
    //    - There can be leaves not associated with any streaming source, because they were
    //      generated from a batch source (e.g. stream-batch joins)
    //      可以有叶子不与任何流式源相关联,因为它们是从批处理源生成的（例如流批处理连接）
    //
    // 2. Assuming that the executed plan has same number of leaves in the same order as that of
    //    the trigger logical plan, we associate executed plan leaves with corresponding
    //    streaming sources.
    //   假设执行的计划具有与触发逻辑计划相同的顺序的叶子数量,我们将执行的计划叶子与相应的流源相关联
    //
    // 3. For each source, we sum the metrics of the associated execution plan leaves.
    //    对于每个来源,我们总结相关的执行计划离开的度量
    //
    val logicalPlanLeafToSource = newData.flatMap { case (source, df) =>
      df.logicalPlan.collectLeaves().map { leaf => leaf -> source }
    }
    val allLogicalPlanLeaves = lastExecution.logical.collectLeaves() // includes non-streaming
    val allExecPlanLeaves = lastExecution.executedPlan.collectLeaves()
    val numInputRows: Map[Source, Long] =
      if (allLogicalPlanLeaves.size == allExecPlanLeaves.size) {
        val execLeafToSource = allLogicalPlanLeaves.zip(allExecPlanLeaves).flatMap {
          case (lp, ep) => logicalPlanLeafToSource.get(lp).map { source => ep -> source }
        }
        val sourceToNumInputRows = execLeafToSource.map { case (execLeaf, source) =>
          val numRows = execLeaf.metrics.get("numOutputRows").map(_.value).getOrElse(0L)
          source -> numRows
        }
        sourceToNumInputRows.groupBy(_._1).mapValues(_.map(_._2).sum) // sum up rows for each source
      } else {
        if (!metricWarningLogged) {
          def toString[T](seq: Seq[T]): String = s"(size = ${seq.size}), ${seq.mkString(", ")}"
          logWarning(
            "Could not report metrics as number leaves in trigger logical plan did not match that" +
                s" of the execution plan:\n" +
                s"logical plan leaves: ${toString(allLogicalPlanLeaves)}\n" +
                s"execution plan leaves: ${toString(allExecPlanLeaves)}\n")
          metricWarningLogged = true
        }
        Map.empty
      }

    val eventTimeStats = lastExecution.executedPlan.collect {
      case e: EventTimeWatermarkExec if e.eventTimeStats.value.count > 0 =>
        val stats = e.eventTimeStats.value
        Map(
          "max" -> stats.max,
          "min" -> stats.min,
          "avg" -> stats.avg.toLong).mapValues(formatTimestamp)
    }.headOption.getOrElse(Map.empty) ++ watermarkTimestamp

    ExecutionStats(numInputRows, stateOperators, eventTimeStats)
  }

  /** Records the duration of running `body` for the next query progress update. */
  protected def reportTimeTaken[T](triggerDetailKey: String)(body: => T): T = {
    val startTime = triggerClock.getTimeMillis()
    val result = body
    val endTime = triggerClock.getTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    val previousTime = currentDurationsMs.getOrElse(triggerDetailKey, 0L)
    currentDurationsMs.put(triggerDetailKey, previousTime + timeTaken)
    logDebug(s"$triggerDetailKey took $timeTaken ms")
    result
  }

  private def formatTimestamp(millis: Long): String = {
    timestampFormat.format(new Date(millis))
  }

  /** Updates the message returned in `status`. */
  protected def updateStatusMessage(message: String): Unit = {
    currentStatus = currentStatus.copy(message = message)
  }
}
