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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, StreamTest}
import org.apache.spark.sql.test.SharedSQLContext

class ForeachSinkSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }
  //用“追加”输出模式
  test("foreach() with `append` output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(2).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
        .outputMode(OutputMode.Append)
        .foreach(new TestForeachWriter())
        .start()

      // -- batch 0 ---------------------------------------
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      var expectedEventsForPartition0 = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 1),
        ForeachSinkSuite.Process(value = 3),
        ForeachSinkSuite.Close(None)
      )
      var expectedEventsForPartition1 = Seq(
        ForeachSinkSuite.Open(partition = 1, version = 0),
        ForeachSinkSuite.Process(value = 2),
        ForeachSinkSuite.Process(value = 4),
        ForeachSinkSuite.Close(None)
      )

      var allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 2)
      assert(allEvents.toSet === Set(expectedEventsForPartition0, expectedEventsForPartition1))

      ForeachSinkSuite.clear()

      // -- batch 1 ---------------------------------------
      input.addData(5, 6, 7, 8)
      query.processAllAvailable()

      expectedEventsForPartition0 = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 1),
        ForeachSinkSuite.Process(value = 5),
        ForeachSinkSuite.Process(value = 7),
        ForeachSinkSuite.Close(None)
      )
      expectedEventsForPartition1 = Seq(
        ForeachSinkSuite.Open(partition = 1, version = 1),
        ForeachSinkSuite.Process(value = 6),
        ForeachSinkSuite.Process(value = 8),
        ForeachSinkSuite.Close(None)
      )

      allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 2)
      assert(allEvents.toSet === Set(expectedEventsForPartition0, expectedEventsForPartition1))

      query.stop()
    }
  }
  //`完整`输出模式“
  test("foreach() with `complete` output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]

      val query = input.toDS()
        .groupBy().count().as[Long].map(_.toInt)
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
        .outputMode(OutputMode.Complete)
        .foreach(new TestForeachWriter())
        .start()

      // -- batch 0 ---------------------------------------
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      var allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      var expectedEvents = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 4),
        ForeachSinkSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))

      ForeachSinkSuite.clear()

      // -- batch 1 ---------------------------------------
      input.addData(5, 6, 7, 8)
      query.processAllAvailable()

      allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      expectedEvents = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 1),
        ForeachSinkSuite.Process(value = 8),
        ForeachSinkSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))

      query.stop()
    }
  }
  //foreach有错误
  testQuietly("foreach with error") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(value)
            throw new RuntimeException("error")
          }
        }).start()
      input.addData(1, 2, 3, 4)

      // Error in `process` should fail the Spark job
      //`process`中的错误将导致Spark作业失败
      val e = intercept[StreamingQueryException] {
        query.processAllAvailable()
      }
      assert(e.getCause.isInstanceOf[SparkException])
      assert(e.getCause.getCause.getMessage === "error")
      assert(query.isActive === false)

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      assert(allEvents(0)(1) === ForeachSinkSuite.Process(value = 1))

      // `close` should be called with the error
      //应该调用close
      val errorEvent = allEvents(0)(2).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.error.get.isInstanceOf[RuntimeException])
      assert(errorEvent.error.get.getMessage === "error")
    }
  }
  //带水印的foreach：完整
  //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
  test("foreach with watermark: complete") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"count".as[Long])
      .map(_.toInt)
      .repartition(1)

    val query = windowedAggregation
      .writeStream
      //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
      .outputMode(OutputMode.Complete)
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      val expectedEvents = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 3),
        ForeachSinkSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))
    } finally {
      query.stop()
    }
  }
  //foreach with watermark:附加
  //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
  test("foreach with watermark: append") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"count".as[Long])
      .map(_.toInt)
      .repartition(1)

    val query = windowedAggregation
      .writeStream
      //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
      .outputMode(OutputMode.Append)
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()
      //将水印提前至15秒
      inputData.addData(25) // Advance watermark to 15 seconds
      query.processAllAvailable()
      //将项目不比以前的水印
      inputData.addData(25) // Evict items less than previous watermark
      query.processAllAvailable()

      // There should be 3 batches and only does the last batch contain a value.
      //应该有3个批次,只有最后一批包含一个值
      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 3)
      val expectedEvents = Seq(
        Seq(
          ForeachSinkSuite.Open(partition = 0, version = 0),
          ForeachSinkSuite.Close(None)
        ),
        Seq(
          ForeachSinkSuite.Open(partition = 0, version = 1),
          ForeachSinkSuite.Close(None)
        ),
        Seq(
          ForeachSinkSuite.Open(partition = 0, version = 2),
          ForeachSinkSuite.Process(value = 3),
          ForeachSinkSuite.Close(None)
        )
      )
      assert(allEvents === expectedEvents)
    } finally {
      query.stop()
    }
  }
  //foreach接收器应该支持度量标准
  test("foreach sink should support metrics") {
    val inputData = MemoryStream[Int]
    val query = inputData.toDS()
      .writeStream
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()
      val recentProgress = query.recentProgress.filter(_.numInputRows != 0).headOption
      assert(recentProgress.isDefined && recentProgress.get.numInputRows === 3,
        s"recentProgress[${query.recentProgress.toList}] doesn't contain correct metrics")
    } finally {
      query.stop()
    }
  }
}

/** A global object to collect events in the executor
  * 在执行者中收集事件的全局对象*/
object ForeachSinkSuite {

  trait Event

  case class Open(partition: Long, version: Long) extends Event

  case class Process[T](value: T) extends Event

  case class Close(error: Option[Throwable]) extends Event

  private val _allEvents = new ConcurrentLinkedQueue[Seq[Event]]()

  def addEvents(events: Seq[Event]): Unit = {
    _allEvents.add(events)
  }

  def allEvents(): Seq[Seq[Event]] = {
    _allEvents.toArray(new Array[Seq[Event]](_allEvents.size()))
  }

  def clear(): Unit = {
    _allEvents.clear()
  }
}

/** A [[ForeachWriter]] that writes collected events to ForeachSinkSuite
  * 一个foreachwriter写收集的事件foreachsinksuite*/
class TestForeachWriter extends ForeachWriter[Int] {
  ForeachSinkSuite.clear()

  private val events = mutable.ArrayBuffer[ForeachSinkSuite.Event]()

  override def open(partitionId: Long, version: Long): Boolean = {
    events += ForeachSinkSuite.Open(partition = partitionId, version = version)
    true
  }

  override def process(value: Int): Unit = {
    events += ForeachSinkSuite.Process(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    events += ForeachSinkSuite.Close(error = Option(errorOrNull))
    ForeachSinkSuite.addEvents(events)
  }
}
