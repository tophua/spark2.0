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

import java.util.concurrent.atomic.AtomicInteger
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, Statistics}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


object MemoryStream {
  protected val currentBlockId = new AtomicInteger(0)
  protected val memoryStreamId = new AtomicInteger(0)

  def apply[A : Encoder](implicit sqlContext: SQLContext): MemoryStream[A] =
    new MemoryStream[A](memoryStreamId.getAndIncrement(), sqlContext)
}

/**
 * A [[Source]] that produces value stored in memory as they are added by the user.  This [[Source]]
 * is intended for use in unit tests as it can only replay data when the object is still
 * available.
  * 一种[Source],当用户添加时,生成存储在内存中的值,
  * 这个[Source]是用于单元测试的,因为它只能在对象仍然可用时重播数据
 */
case class MemoryStream[A : Encoder](id: Int, sqlContext: SQLContext)
    extends Source with Logging {
  protected val encoder = encoderFor[A]
  protected val logicalPlan = StreamingExecutionRelation(this, sqlContext.sparkSession)
  protected val output = logicalPlan.output

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
    * 所有批次` lastcommittedoffset + 1 `到` currentoffset `,包容,存储在ListBuffer促进消除提交的批。
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[Dataset[A]]

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  /**
   * Last offset that was discarded, or -1 if no commits have occurred. Note that the value
   * -1 is used in calculations below and isn't just an arbitrary constant.
    * 最后一个被丢弃的偏移量,或者如果没有提交,则为1。
    * 注意,下面的计算中使用了值- 1,而不只是一个任意常量。
   */
  @GuardedBy("this")
  protected var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  def schema: StructType = encoder.schema

  def toDS(): Dataset[A] = {
    Dataset(sqlContext.sparkSession, logicalPlan)
  }

  def toDF(): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, logicalPlan)
  }

  def addData(data: A*): Offset = {
    addData(data.toTraversable)
  }

  def addData(data: TraversableOnce[A]): Offset = {
    val encoded = data.toVector.map(d => encoder.toRow(d).copy())
    val plan = new LocalRelation(schema.toAttributes, encoded, isStreaming = true)
    val ds = Dataset[A](sqlContext.sparkSession, plan)
    logDebug(s"Adding ds: $ds")
    this.synchronized {
      currentOffset = currentOffset + 1
      batches += ds
      currentOffset
    }
  }

  override def toString: String = s"MemoryStream[${Utils.truncatedString(output, ",")}]"

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1) {
      None
    } else {
      Some(currentOffset)
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Compute the internal batch numbers to fetch: [startOrdinal, endOrdinal)
    val startOrdinal =
      start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset.toInt + 1

    // Internal buffer only holds the batches after lastCommittedOffset.
    //内部缓冲区仅包含lastCommittedOffset后的批处理
    val newBlocks = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    logDebug(generateDebugString(newBlocks, startOrdinal, endOrdinal))

    newBlocks
      .map(_.toDF())
      .reduceOption(_ union _)
      .getOrElse {
        sys.error("No data selected!")
      }
  }

  private def generateDebugString(
      blocks: TraversableOnce[Dataset[A]],
      startOrdinal: Int,
      endOrdinal: Int): String = {
    val originalUnsupportedCheck =
      sqlContext.getConf("spark.sql.streaming.unsupportedOperationCheck")
    try {
      sqlContext.setConf("spark.sql.streaming.unsupportedOperationCheck", "false")
      s"MemoryBatch [$startOrdinal, $endOrdinal]: " +
          s"${blocks.flatMap(_.collect()).mkString(", ")}"
    } finally {
      sqlContext.setConf("spark.sql.streaming.unsupportedOperationCheck", originalUnsupportedCheck)
    }
  }

  override def commit(end: Offset): Unit = synchronized {
    def check(newOffset: LongOffset): Unit = {
      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    }

    LongOffset.convert(end) match {
      case Some(lo) => check(lo)
      case None => sys.error(s"MemoryStream.commit() received an offset ($end) " +
        "that did not originate with an instance of this class")
    }
  }

  override def stop() {}

  def reset(): Unit = synchronized {
    batches.clear()
    currentOffset = new LongOffset(-1)
    lastOffsetCommitted = new LongOffset(-1)
  }
}

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
  * 将结果存储在内存中的接收器,这个[[Sink]]主要用于单元测试,不提供耐久性
 */
class MemorySink(val schema: StructType, outputMode: OutputMode) extends Sink with Logging {

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]].
    * 已写入此[Sink]的批订单列表 */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]].
    * 返回存储在这个[Sink]中的所有行*/
  def allData: Seq[Row] = synchronized {
    batches.map(_.data).flatten
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized { batches.lastOption.toSeq.flatten(_.data) }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case Append | Update =>
          val rows = AddedData(batchId, data.collect())
          synchronized { batches += rows }

        case Complete =>
          val rows = AddedData(batchId, data.collect())
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "MemorySink"
}

/**
 * Used to query the data that has been written into a [[MemorySink]].
  * 用于查询数据已写入memorysink
 */
case class MemoryPlan(sink: MemorySink, output: Seq[Attribute]) extends LeafNode {
  def this(sink: MemorySink) = this(sink, sink.schema.toAttributes)

  private val sizePerRow = sink.schema.toAttributes.map(_.dataType.defaultSize).sum

  override def computeStats(): Statistics = Statistics(sizePerRow * sink.allData.size)
}
