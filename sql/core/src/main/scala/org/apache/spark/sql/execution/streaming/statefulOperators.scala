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
import java.util.concurrent.TimeUnit._

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{OutputMode, StateOperatorProgress}
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, NextIterator}


/** Used to identify the state store for a given operator.
  * 用于识别给定操作员的状态存储*/
case class StatefulOperatorStateInfo(
    checkpointLocation: String,
    queryRunId: UUID,
    operatorId: Long,
    storeVersion: Long)

/**
 * An operator that reads or writes state from the [[StateStore]].
  * 从[[StateStore]]读取或写入状态的操作员
 * The [[StatefulOperatorStateInfo]] should be filled in by `prepareForExecution` in
 * [[IncrementalExecution]].
  * [[StatefulOperatorStateInfo]]应该由[[IncrementalExecution]]中的prepareForExecution填充
 */
trait StatefulOperator extends SparkPlan {
  def stateInfo: Option[StatefulOperatorStateInfo]

  protected def getStateInfo: StatefulOperatorStateInfo = attachTree(this) {
    stateInfo.getOrElse {
      throw new IllegalStateException("State location not present for execution")
    }
  }
}

/** An operator that reads from a StateStore.
  * 从StateStore读取的操作符 */
trait StateStoreReader extends StatefulOperator {
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))
}

/** An operator that writes to a StateStore.
  * 写入StateStore的操作符*/
trait StateStoreWriter extends StatefulOperator { self: SparkPlan =>

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numTotalStateRows" -> SQLMetrics.createMetric(sparkContext, "number of total state rows"),
    "numUpdatedStateRows" -> SQLMetrics.createMetric(sparkContext, "number of updated state rows"),
    "allUpdatesTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "total time to update rows"),
    "allRemovalsTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "total time to remove rows"),
    "commitTimeMs" -> SQLMetrics.createTimingMetric(sparkContext, "time to commit changes"),
    "stateMemory" -> SQLMetrics.createSizeMetric(sparkContext, "memory used by state")
  ) ++ stateStoreCustomMetrics

  /**
   * Get the progress made by this stateful operator after execution. This should be called in
   * the driver after this SparkPlan has been executed and metrics have been updated.
    * 在执行之后获取这个有状态操作符所取得的进展,在SparkPlan执行完毕并且指标已经更新之后,这应该在驱动程序中调用
   */
  def getProgress(): StateOperatorProgress = {
    new StateOperatorProgress(
      numRowsTotal = longMetric("numTotalStateRows").value,
      numRowsUpdated = longMetric("numUpdatedStateRows").value,
      memoryUsedBytes = longMetric("stateMemory").value)
  }

  /** Records the duration of running `body` for the next query progress update.
    * 为下一个查询进程更新记录运行`body`的持续时间*/
  protected def timeTakenMs(body: => Unit): Long = {
    val startTime = System.nanoTime()
    val result = body
    val endTime = System.nanoTime()
    math.max(NANOSECONDS.toMillis(endTime - startTime), 0)
  }

  /**
   * Set the SQL metrics related to the state store.
    * 设置与状态存储相关的SQL度量标准
   * This should be called in that task after the store has been updated.
    * 在存储更新之后,应该在这个任务中调用它
   */
  protected def setStoreMetrics(store: StateStore): Unit = {
    val storeMetrics = store.metrics
    longMetric("numTotalStateRows") += storeMetrics.numKeys
    longMetric("stateMemory") += storeMetrics.memoryUsedBytes
    storeMetrics.customMetrics.foreach { case (metric, value) =>
      longMetric(metric.name) += value
    }
  }

  private def stateStoreCustomMetrics: Map[String, SQLMetric] = {
    val provider = StateStoreProvider.create(sqlContext.conf.stateStoreProviderClass)
    provider.supportedCustomMetrics.map {
      case StateStoreCustomSizeMetric(name, desc) =>
        name -> SQLMetrics.createSizeMetric(sparkContext, desc)
      case StateStoreCustomTimingMetric(name, desc) =>
        name -> SQLMetrics.createTimingMetric(sparkContext, desc)
    }.toMap
  }
}

/** An operator that supports watermark.
  * 支持水印的运算符*/
trait WatermarkSupport extends UnaryExecNode {

  /** The keys that may have a watermark attribute.
    * 可能具有水印属性的键*/
  def keyExpressions: Seq[Attribute]

  /** The watermark value.
    * 水印值*/
  def eventTimeWatermark: Option[Long]

  /** Generate an expression that matches data older than the watermark
    * 生成一个匹配比水印更早的数据的表达式 */
  lazy val watermarkExpression: Option[Expression] = {
    val optionalWatermarkAttribute =
      child.output.find(_.metadata.contains(EventTimeWatermark.delayKey))

    optionalWatermarkAttribute.map { watermarkAttribute =>
      // If we are evicting based on a window, use the end of the window.  Otherwise just
      // use the attribute itself.
      //如果我们是基于窗口驱逐的话,请使用窗口的末尾,否则,只使用属性本身,
      val evictionExpression =
        if (watermarkAttribute.dataType.isInstanceOf[StructType]) {
          LessThanOrEqual(
            GetStructField(watermarkAttribute, 1),
            Literal(eventTimeWatermark.get * 1000))
        } else {
          LessThanOrEqual(
            watermarkAttribute,
            Literal(eventTimeWatermark.get * 1000))
        }

      logInfo(s"Filtering state store on: $evictionExpression")
      evictionExpression
    }
  }

  /** Predicate based on keys that matches data older than the watermark
    * 基于匹配比水印更早的数据的键来判断 */
  lazy val watermarkPredicateForKeys: Option[Predicate] = watermarkExpression.flatMap { e =>
    if (keyExpressions.exists(_.metadata.contains(EventTimeWatermark.delayKey))) {
      Some(newPredicate(e, keyExpressions))
    } else {
      None
    }
  }

  /** Predicate based on the child output that matches data older than the watermark.
    * 基于比水印更早的数据匹配的子输出的谓词*/
  lazy val watermarkPredicateForData: Option[Predicate] =
    watermarkExpression.map(newPredicate(_, child.output))

  protected def removeKeysOlderThanWatermark(store: StateStore): Unit = {
    if (watermarkPredicateForKeys.nonEmpty) {
      store.getRange(None, None).foreach { rowPair =>
        if (watermarkPredicateForKeys.get.eval(rowPair.key)) {
          store.remove(rowPair.key)
        }
      }
    }
  }
}

/**
 * For each input tuple, the key is calculated and the value from the [[StateStore]] is added
 * to the stream (in addition to the input tuple) if present.
  * 对于每个输入元组,计算Key并将[[StateStore]]中的值添加到流(除了输入元组之外)（如果存在）
 */
case class StateStoreRestoreExec(
    keyExpressions: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo],
    child: SparkPlan)
  extends UnaryExecNode with StateStoreReader {

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      child.output.toStructType,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        iter.flatMap { row =>
          val key = getKey(row)
          val savedState = store.get(key)
          numOutputRows += 1
          row +: Option(savedState).toSeq
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * For each input tuple, the key is calculated and the tuple is `put` into the [[StateStore]].
  * 对于每个输入元组,计算Key,并将元组“放入”[[StateStore]]
 */
case class StateStoreSaveExec(
    keyExpressions: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    outputMode: Option[OutputMode] = None,
    eventTimeWatermark: Option[Long] = None,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    assert(outputMode.nonEmpty,
      "Incorrect planning in IncrementalExecution, outputMode has not been set")

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      child.output.toStructType,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { (store, iter) =>
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        val numOutputRows = longMetric("numOutputRows")
        val numUpdatedStateRows = longMetric("numUpdatedStateRows")
        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
        val commitTimeMs = longMetric("commitTimeMs")

        outputMode match {
          // Update and output all rows in the StateStore.
          case Some(Complete) =>
            allUpdatesTimeMs += timeTakenMs {
              while (iter.hasNext) {
                val row = iter.next().asInstanceOf[UnsafeRow]
                val key = getKey(row)
                store.put(key, row)
                numUpdatedStateRows += 1
              }
            }
            allRemovalsTimeMs += 0
            commitTimeMs += timeTakenMs {
              store.commit()
            }
            setStoreMetrics(store)
            store.iterator().map { rowPair =>
              numOutputRows += 1
              rowPair.value
            }

          // Update and output only rows being evicted from the StateStore
            //只更新并输出从StateStore被逐出的行
          // Assumption: watermark predicates must be non-empty if append mode is allowed
          case Some(Append) =>
            allUpdatesTimeMs += timeTakenMs {
              val filteredIter = iter.filter(row => !watermarkPredicateForData.get.eval(row))
              while (filteredIter.hasNext) {
                val row = filteredIter.next().asInstanceOf[UnsafeRow]
                val key = getKey(row)
                store.put(key, row)
                numUpdatedStateRows += 1
              }
            }

            val removalStartTimeNs = System.nanoTime
            val rangeIter = store.getRange(None, None)

            new NextIterator[InternalRow] {
              override protected def getNext(): InternalRow = {
                var removedValueRow: InternalRow = null
                while(rangeIter.hasNext && removedValueRow == null) {
                  val rowPair = rangeIter.next()
                  if (watermarkPredicateForKeys.get.eval(rowPair.key)) {
                    store.remove(rowPair.key)
                    removedValueRow = rowPair.value
                  }
                }
                if (removedValueRow == null) {
                  finished = true
                  null
                } else {
                  removedValueRow
                }
              }

              override protected def close(): Unit = {
                allRemovalsTimeMs += NANOSECONDS.toMillis(System.nanoTime - removalStartTimeNs)
                commitTimeMs += timeTakenMs { store.commit() }
                setStoreMetrics(store)
              }
            }

          // Update and output modified rows from the StateStore.
            //从StateStore更新并输出修改的行
          case Some(Update) =>

            val updatesStartTimeNs = System.nanoTime

            new Iterator[InternalRow] {

              // Filter late date using watermark if specified
              //如果指定,使用水印过滤日期
              private[this] val baseIterator = watermarkPredicateForData match {
                case Some(predicate) => iter.filter((row: InternalRow) => !predicate.eval(row))
                case None => iter
              }

              override def hasNext: Boolean = {
                if (!baseIterator.hasNext) {
                  allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)

                  // Remove old aggregates if watermark specified
                  //如果指定了水印,请移除旧聚合
                  allRemovalsTimeMs += timeTakenMs { removeKeysOlderThanWatermark(store) }
                  commitTimeMs += timeTakenMs { store.commit() }
                  setStoreMetrics(store)
                  false
                } else {
                  true
                }
              }

              override def next(): InternalRow = {
                val row = baseIterator.next().asInstanceOf[UnsafeRow]
                val key = getKey(row)
                store.put(key, row)
                numOutputRows += 1
                numUpdatedStateRows += 1
                row
              }
            }

          case _ => throw new UnsupportedOperationException(s"Invalid output mode: $outputMode")
        }
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/** Physical operator for executing streaming Deduplicate.
  * 物理运营商用于执行流式重复数据删除 */
case class StreamingDeduplicateExec(
    keyExpressions: Seq[Attribute],
    child: SparkPlan,
    stateInfo: Option[StatefulOperatorStateInfo] = None,
    eventTimeWatermark: Option[Long] = None)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  /** Distribute by grouping attributes
    * 通过分组属性进行分配*/
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(keyExpressions) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver
    //在驱动程序强制延迟初始化

    child.execute().mapPartitionsWithStateStore(
      getStateInfo,
      keyExpressions.toStructType,
      child.output.toStructType,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { (store, iter) =>
      val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
      val numOutputRows = longMetric("numOutputRows")
      val numTotalStateRows = longMetric("numTotalStateRows")
      val numUpdatedStateRows = longMetric("numUpdatedStateRows")
      val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
      val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")
      val commitTimeMs = longMetric("commitTimeMs")

      val baseIterator = watermarkPredicateForData match {
        case Some(predicate) => iter.filter(row => !predicate.eval(row))
        case None => iter
      }

      val updatesStartTimeNs = System.nanoTime

      val result = baseIterator.filter { r =>
        val row = r.asInstanceOf[UnsafeRow]
        val key = getKey(row)
        val value = store.get(key)
        if (value == null) {
          store.put(key, StreamingDeduplicateExec.EMPTY_ROW)
          numUpdatedStateRows += 1
          numOutputRows += 1
          true
        } else {
          // Drop duplicated rows
          false
        }
      }

      CompletionIterator[InternalRow, Iterator[InternalRow]](result, {
        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
        allRemovalsTimeMs += timeTakenMs { removeKeysOlderThanWatermark(store) }
        commitTimeMs += timeTakenMs { store.commit() }
        setStoreMetrics(store)
      })
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

object StreamingDeduplicateExec {
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))
}
