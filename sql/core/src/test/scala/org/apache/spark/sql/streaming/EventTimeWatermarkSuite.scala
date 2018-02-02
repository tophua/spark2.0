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

package org.apache.spark.sql.streaming

import java.{util => ju}
import java.text.SimpleDateFormat
import java.util.Date

import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.OutputMode._

class EventTimeWatermarkSuite extends StreamTest with BeforeAndAfter with Matchers with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }
 //事件时间统计
  test("EventTimeStats") {
    val epsilon = 10E-6

    val stats = EventTimeStats(max = 100, min = 10, avg = 20.0, count = 5)
    stats.add(80L)
    stats.max should be (100)
    stats.min should be (10)
    stats.avg should be (30.0 +- epsilon)
    stats.count should be (6)

    val stats2 = EventTimeStats(80L, 5L, 15.0, 4)
    stats.merge(stats2)
    stats.max should be (100)
    stats.min should be (5)
    stats.avg should be (24.0 +- epsilon)
    stats.count should be (10)
  }
  //事件时间统计:平均值大
  test("EventTimeStats: avg on large values") {
    val epsilon = 10E-6
    val largeValue = 10000000000L // 10B
    // Make sure `largeValue` will cause overflow if we use a Long sum to calc avg.
    //确保`largeValue`会导致溢出,如果我们使用长期平均计算
    assert(largeValue * largeValue != BigInt(largeValue) * BigInt(largeValue))
    val stats =
      EventTimeStats(max = largeValue, min = largeValue, avg = largeValue, count = largeValue - 1)
    stats.add(largeValue)
    stats.avg should be (largeValue.toDouble +- epsilon)

    val stats2 = EventTimeStats(
      max = largeValue + 1,
      min = largeValue,
      avg = largeValue + 1,
      count = largeValue)
    stats.merge(stats2)
    stats.avg should be ((largeValue + 0.5) +- epsilon)
  }
  //错误列错误
  test("error on bad column") {
    val inputData = MemoryStream[Int].toDF()
    val e = intercept[AnalysisException] {
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      inputData.withWatermark("badColumn", "1 minute")
    }
    assert(e.getMessage contains "badColumn")
  }
  //错误的类型错误
  test("error on wrong type") {
    val inputData = MemoryStream[Int].toDF()
    val e = intercept[AnalysisException] {
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      inputData.withWatermark("value", "1 minute")
    }
    assert(e.getMessage contains "value")
    assert(e.getMessage contains "int")
  }
  //事件时间和水印指标
  test("event time and watermark metrics") {
    // No event time metrics when there is no watermarking
    //无水印时无事件时间度量
    val inputData1 = MemoryStream[Int]
    val aggWithoutWatermark = inputData1.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(aggWithoutWatermark, outputMode = Complete)(
      AddData(inputData1, 15),
      CheckAnswer((15, 1)),
      assertEventStats { e => assert(e.isEmpty) },
      AddData(inputData1, 10, 12, 14),
      CheckAnswer((10, 3), (15, 1)),
      assertEventStats { e => assert(e.isEmpty) }
    )

    // All event time metrics where watermarking is set
    //所有设置水印的事件时间指标
    val inputData2 = MemoryStream[Int]
    val aggWithWatermark = inputData2.toDF()
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
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(aggWithWatermark)(
      AddData(inputData2, 15),
      CheckAnswer(),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(15))
        assert(e.get("min") === formatTimestamp(15))
        assert(e.get("avg") === formatTimestamp(15))
        assert(e.get("watermark") === formatTimestamp(0))
      },
      AddData(inputData2, 10, 12, 14),
      CheckAnswer(),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(14))
        assert(e.get("min") === formatTimestamp(10))
        assert(e.get("avg") === formatTimestamp(12))
        assert(e.get("watermark") === formatTimestamp(5))
      },
      AddData(inputData2, 25),
      CheckAnswer(),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(25))
        assert(e.get("min") === formatTimestamp(25))
        assert(e.get("avg") === formatTimestamp(25))
        assert(e.get("watermark") === formatTimestamp(5))
      },
      AddData(inputData2, 25),
      CheckAnswer((10, 3)),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(25))
        assert(e.get("min") === formatTimestamp(25))
        assert(e.get("avg") === formatTimestamp(25))
        assert(e.get("watermark") === formatTimestamp(15))
      }
    )
  }
  //追加模式
  //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
  test("append mode") {
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
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckLastBatch(),
      //将水印提前15秒
      AddData(inputData, 25),   // Advance watermark to 15 seconds
      CheckLastBatch(),
      assertNumStateRows(3),
      //发出小于水印的项目并放弃其状态
      AddData(inputData, 25),   // Emit items less than watermark and drop their state
      CheckLastBatch((10, 5)),
      assertNumStateRows(2),
      //不应该以小于水印的数据发射任何东西
      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(2)
    )
  }
  //更新模式
  test("update mode") {
    val inputData = MemoryStream[Int]
    spark.conf.set("spark.sql.shuffle.partitions", "10")

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
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation, OutputMode.Update)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckLastBatch((10, 5), (15, 1)),
      //将水印提前15秒
      AddData(inputData, 25),     // Advance watermark to 15 seconds
      CheckLastBatch((25, 1)),
      assertNumStateRows(3),
      //忽略10作为其小于水印
      AddData(inputData, 10, 25), // Ignore 10 as its less than watermark
      CheckLastBatch((25, 2)),
      assertNumStateRows(2),
      //不应该以小于水印的数据发射任何东西
      AddData(inputData, 10),     // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(2)
    )
  }
  //延迟数月和数年处理正确
  test("delay in months and years handled correctly") {
    val currentTimeMs = System.currentTimeMillis
    val currentTime = new Date(currentTimeMs)

    val input = MemoryStream[Long]
    val aggWithWatermark = input.toDF()
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
      .withWatermark("eventTime", "2 years 5 months")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    def monthsSinceEpoch(date: Date): Int = { date.getYear * 12 + date.getMonth }

    testStream(aggWithWatermark)(
      AddData(input, currentTimeMs / 1000),
      CheckAnswer(),
      AddData(input, currentTimeMs / 1000),
      CheckAnswer(),
      assertEventStats { e =>
        assert(timestampFormat.parse(e.get("max")).getTime === (currentTimeMs / 1000) * 1000)
        val watermarkTime = timestampFormat.parse(e.get("watermark"))
        val monthDiff = monthsSinceEpoch(currentTime) - monthsSinceEpoch(watermarkTime)
        // monthsSinceEpoch is like `math.floor(num)`, so monthDiff has two possible values.
        //
        assert(monthDiff === 29 || monthDiff === 30,
          s"currentTime: $currentTime, watermarkTime: $watermarkTime")
      }
    )
  }
  //恢复
  test("recovery") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
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
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(df)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckLastBatch(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      StopStream,
      StartStream(),
      CheckLastBatch(),
      //将项目不比以前的水印
      AddData(inputData, 25), // Evict items less than previous watermark.
      CheckLastBatch((10, 5)),
      StopStream,
      AssertOnQuery { q => // purge commit and clear the sink
        val commit = q.batchCommitLog.getLatest().map(_._1).getOrElse(-1L) + 1L
        q.batchCommitLog.purge(commit)
        q.sink.asInstanceOf[MemorySink].clear()
        true
      },
      StartStream(),
      //重新计算和重新将时间戳的最后一批10
      CheckLastBatch((10, 5)), // Recompute last batch and re-evict timestamp 10
      //将水印提前至20秒
      AddData(inputData, 30), // Advance watermark to 20 seconds
      CheckLastBatch(),
      StopStream,
      //水印应该仍然是15秒
      StartStream(), // Watermark should still be 15 seconds
      AddData(inputData, 17),
      //我们仍然没有看到下一批
      CheckLastBatch(), // We still do not see next batch
      //将水印提前至20秒
      AddData(inputData, 30), // Advance watermark to 20 seconds
      CheckLastBatch(),
      //将项目不比以前的水印
      AddData(inputData, 30), // Evict items less than previous watermark.
      //确保我们看到下一个窗口
      CheckLastBatch((15, 2)) // Ensure we see next window
    )
  }
  //丢弃旧的数据
  test("dropping old data") {
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
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12),
      CheckAnswer(),
      //将水印提前至15秒
      AddData(inputData, 25),     // Advance watermark to 15 seconds
      CheckAnswer(),
      //将项目不比以前的水印
      AddData(inputData, 25),     // Evict items less than previous watermark.
      CheckAnswer((10, 3)),
      //10比15秒水印晚
      AddData(inputData, 10),     // 10 is later than 15 second watermark
      CheckAnswer((10, 3)),
      AddData(inputData, 25),
      //不应该发出不正确的部分结果
      CheckAnswer((10, 3))        // Should not emit an incorrect partial result.
    )
  }
//2流水印
  test("watermark with 2 streams") {
    import org.apache.spark.sql.functions.sum
    val first = MemoryStream[Int]

    val firstDf = first.toDF()
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
      .select('value)

    val second = MemoryStream[Int]

    val secondDf = second.toDF()
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
      .withWatermark("eventTime", "5 seconds")
      .select('value)

    withTempDir { checkpointDir =>
      val unionWriter = firstDf.union(secondDf).agg(sum('value))
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
        .outputMode("complete")
        .queryName("test")

      val union = unionWriter.start()

      def getWatermarkAfterData(
                                 firstData: Seq[Int] = Seq.empty,
                                 secondData: Seq[Int] = Seq.empty,
                                 query: StreamingQuery = union): Long = {
        if (firstData.nonEmpty) first.addData(firstData)
        if (secondData.nonEmpty) second.addData(secondData)
        query.processAllAvailable()
        // add a dummy batch so lastExecution has the new watermark
        //添加一个虚拟批量lastexecution有新的水印
        first.addData(0)
        query.processAllAvailable()
        // get last watermark
        val lastExecution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
        lastExecution.offsetSeqMetadata.batchWatermarkMs
      }

      // Global watermark starts at 0 until we get data from both sides
      //全局水印在0开始,直到我们从双方获得数据。
      assert(getWatermarkAfterData(firstData = Seq(11)) == 0)
      assert(getWatermarkAfterData(secondData = Seq(6)) == 1000)
      // Global watermark stays at left watermark 1 when right watermark moves to 2
      //当右水印移动到2时,全局水印保持在左水印1
      assert(getWatermarkAfterData(secondData = Seq(8)) == 1000)
      // Global watermark switches to right side value 2 when left watermark goes higher
      //当左水印变高时，全局水印切换到右侧值2
      assert(getWatermarkAfterData(firstData = Seq(21)) == 3000)
      // Global watermark goes back to left
      assert(getWatermarkAfterData(secondData = Seq(17, 28, 39)) == 11000)
      // Global watermark stays on left as long as it's below right
      assert(getWatermarkAfterData(firstData = Seq(31)) == 21000)
      assert(getWatermarkAfterData(firstData = Seq(41)) == 31000)
      // Global watermark switches back to right again
      assert(getWatermarkAfterData(firstData = Seq(51)) == 34000)

      // Global watermark is updated correctly with simultaneous data from both sides
      assert(getWatermarkAfterData(firstData = Seq(100), secondData = Seq(100)) == 90000)
      assert(getWatermarkAfterData(firstData = Seq(120), secondData = Seq(110)) == 105000)
      assert(getWatermarkAfterData(firstData = Seq(130), secondData = Seq(125)) == 120000)

      // Global watermark doesn't decrement with simultaneous data
      assert(getWatermarkAfterData(firstData = Seq(100), secondData = Seq(100)) == 120000)
      assert(getWatermarkAfterData(firstData = Seq(140), secondData = Seq(100)) == 120000)
      assert(getWatermarkAfterData(firstData = Seq(100), secondData = Seq(135)) == 130000)

      // Global watermark recovers after restart, but left side watermark ahead of it does not.
      assert(getWatermarkAfterData(firstData = Seq(200), secondData = Seq(190)) == 185000)
      union.stop()
      val union2 = unionWriter.start()
      assert(getWatermarkAfterData(query = union2) == 185000)
      // Even though the left side was ahead of 185000 in the last execution, the watermark won't
      // increment until it gets past it in this execution.
      assert(getWatermarkAfterData(secondData = Seq(200), query = union2) == 185000)
      assert(getWatermarkAfterData(firstData = Seq(200), query = union2) == 190000)
    }
  }
  //完整模式
  //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
  test("complete mode") {
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
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    // No eviction when asked to compute complete results.
    //要求计算完整的结果时不要驱逐
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(windowedAggregation, OutputMode.Complete)(
      AddData(inputData, 10, 11, 12),
      CheckAnswer((10, 3)),
      AddData(inputData, 25),
      CheckAnswer((10, 3), (25, 1)),
      AddData(inputData, 25),
      CheckAnswer((10, 3), (25, 2)),
      AddData(inputData, 10),
      CheckAnswer((10, 4), (25, 2)),
      AddData(inputData, 25),
      CheckAnswer((10, 4), (25, 3))
    )
  }
  //按原始时间戳分组
  test("group by on raw timestamp") {
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
        .groupBy($"eventTime")
        .agg(count("*") as 'count)
        .select($"eventTime".cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10),
      CheckAnswer(),
      //将水印提前15秒
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckAnswer(),
      //驱逐物品少于以前的水印
      AddData(inputData, 25), // Evict items less than previous watermark.
      CheckAnswer((10, 1))
    )
  }
  //延迟阈值不应该是负的
  test("delay threshold should not be negative.") {
    val inputData = MemoryStream[Int].toDF()
    var e = intercept[IllegalArgumentException] {
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      inputData.withWatermark("value", "-1 year")
    }
    assert(e.getMessage contains "should not be negative.")

    e = intercept[IllegalArgumentException] {
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      inputData.withWatermark("value", "1 year -13 months")
    }
    assert(e.getMessage contains "should not be negative.")

    e = intercept[IllegalArgumentException] {
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      inputData.withWatermark("value", "1 month -40 days")
    }
    assert(e.getMessage contains "should not be negative.")

    e = intercept[IllegalArgumentException] {
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      inputData.withWatermark("value", "-10 seconds")
    }
    assert(e.getMessage contains "should not be negative.")
  }
  //新的水印应该覆盖旧的水印
  test("the new watermark should override the old one") {
    val df = MemoryStream[(Long, Long)].toDF()
      .withColumn("first", $"_1".cast("timestamp"))
      .withColumn("second", $"_2".cast("timestamp"))
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      .withWatermark("first", "1 minute")
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      .withWatermark("second", "2 minutes")

    val eventTimeColumns = df.logicalPlan.output
      .filter(_.metadata.contains(EventTimeWatermark.delayKey))
    assert(eventTimeColumns.size === 1)
    assert(eventTimeColumns(0).name === "second")
  }
  //批量查询中应该忽略EventTime水印
  test("EventTime watermark should be ignored in batch query.") {
    val df = testData
      .withColumn("eventTime", $"key".cast("timestamp"))
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      .withWatermark("eventTime", "1 minute")
      .select("eventTime")
      .as[Long]

    checkDataset[Long](df, 1L to 100L: _*)
  }
  //水印运算符接受来自替换的属性
  test("SPARK-21565: watermark operator accepts attributes from replacement") {
    withTempDir { dir =>
      dir.delete()

      val df = Seq(("a", 100.0, new java.sql.Timestamp(100L)))
        .toDF("symbol", "price", "eventTime")
      df.write.json(dir.getCanonicalPath)

      val input = spark.readStream.schema(df.schema)
        .json(dir.getCanonicalPath)

      val groupEvents = input
        /**
          * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
          * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
          * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
          * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
          * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
          * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
          * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
          */
        .withWatermark("eventTime", "2 seconds")
        .groupBy("symbol", "eventTime")
        .agg(count("price") as 'count)
        .select("symbol", "eventTime", "count")
      val q = groupEvents.writeStream
        //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
        .outputMode("append")
        //控制台接收器（用于调试） - 每次有触发器时将输出打印到控制台/ stdout。
        // 这应该用于低数据量上的调试目的,因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("console")
        .start()
      try {
        q.processAllAvailable()
      } finally {
        q.stop()
      }
    }
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }

  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    AssertOnQuery { q =>
      body(q.recentProgress.filter(_.numInputRows > 0).lastOption.get.eventTime)
      true
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone("UTC"))

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }
}
