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

import java.util.{Locale, TimeZone}

import org.scalatest.Assertions
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.StructType

object FailureSinglton {
  var firstTime = true
}
//streaming aggregations （流聚合）
class StreamingAggregationSuite extends StateStoreMetricsTest
    with BeforeAndAfterAll with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  import testImplicits._
  //简单的计数,更新模式
  test("simple count, update mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    testStream(aggregated, Update)(
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 3, 2),
      CheckLastBatch((3, 2), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 3), (2, 2), (1, 1)),
      // By default we run in new tuple mode.
      //默认情况下,我们运行在新的元组模式
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4))
    )
  }
  //将一个字段进行更多行的拆分
  test("count distinct") {
    val inputData = MemoryStream[(Int, Seq[Int])]

    val aggregated =
      inputData.toDF()
        .select($"*", explode($"_2") as 'value)
        .groupBy($"_1")
        .agg(size(collect_set($"value")))
        .as[(Int, Int)]

    testStream(aggregated, Update)(
      AddData(inputData, (1, Seq(1, 2))),
      CheckLastBatch((1, 2))
    )
  }
  //简单计数，完整模式
  ////Complete模式每次触发后，整个结果表将输出到接收器。聚合查询支持此选项。
  test("simple count, complete mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(aggregated, Complete)(
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 2),
      CheckLastBatch((3, 1), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 2), (2, 2), (1, 1)),
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4), (3, 2), (2, 2), (1, 1))
    )
  }
  //简单计数，追加模式
  //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
  //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
  //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
  test("simple count, append mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    val e = intercept[AnalysisException] {
      //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
      //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
      //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
      testStream(aggregated, Append)()
    }
    //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
    //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
    //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
    Seq("append", "not supported").foreach { m =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
    }
  }
  //以完整模式聚合后排序
  test("sort after aggregate in complete mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .toDF("value", "count")
        .orderBy($"count".desc)
        .as[(Int, Long)]
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(aggregated, Complete)(
      AddData(inputData, 3),
      CheckLastBatch(isSorted = true, (3, 1)),
      AddData(inputData, 2, 3),
      CheckLastBatch(isSorted = true, (3, 2), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch(isSorted = true, (3, 3), (2, 2), (1, 1)),
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch(isSorted = true, (4, 4), (3, 3), (2, 2), (1, 1))
    )
  }
  //状态指标
  test("state metrics") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDS()
        .flatMap(x => Seq(x, x + 1))
        .toDF("value")
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    implicit class RichStreamExecution(query: StreamExecution) {
      def stateNodes: Seq[SparkPlan] = {
        query.lastExecution.executedPlan.collect {
          case p if p.isInstanceOf[StateStoreSaveExec] => p
        }
      }
    }

    // Test with Update mode 测试更新模式
    testStream(aggregated, Update)(
      AddData(inputData, 1),
      CheckLastBatch((1, 1), (2, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 2 },
      AddData(inputData, 2, 3),
      CheckLastBatch((2, 2), (3, 2), (4, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 3 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 3 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 4 }
    )

    // Test with Complete mode 测试完成模式
    inputData.reset()
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(aggregated, Complete)(
      AddData(inputData, 1),
      CheckLastBatch((1, 1), (2, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 2 },
      AddData(inputData, 2, 3),
      CheckLastBatch((1, 1), (2, 2), (3, 2), (4, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 4 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 3 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 4 }
    )
  }
  //多个键
  test("multiple keys") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value", $"value" + 1)
        .agg(count("*"))
        .as[(Int, Int, Long)]

    testStream(aggregated, Update)(
      AddData(inputData, 1, 2),
      CheckLastBatch((1, 2, 1), (2, 3, 1)),
      AddData(inputData, 1, 2),
      CheckLastBatch((1, 2, 2), (2, 3, 2))
    )
  }
  //midbatch失败
  testQuietly("midbatch failure") {
    val inputData = MemoryStream[Int]
    FailureSinglton.firstTime = true
    val aggregated =
      inputData.toDS()
          .map { i =>
            if (i == 4 && FailureSinglton.firstTime) {
              FailureSinglton.firstTime = false
              sys.error("injected failure")
            }

            i
          }
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

    testStream(aggregated, Update)(
      StartStream(),
      AddData(inputData, 1, 2, 3, 4),
      ExpectFailure[SparkException](),
      StartStream(),
      CheckLastBatch((1, 1), (2, 1), (3, 1), (4, 1))
    )
  }
  //类型的聚合器
  test("typed aggregators") {
    val inputData = MemoryStream[(String, Int)]
    val aggregated = inputData.toDS().groupByKey(_._1).agg(typed.sumLong(_._2))

    testStream(aggregated, Update)(
      AddData(inputData, ("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)),
      CheckLastBatch(("a", 30), ("b", 3), ("c", 1))
    )
  }
  //通过current_time修剪结果，完成模式
  ignore("prune results by current_time, complete mode") {
    import testImplicits._
    val clock = new StreamManualClock
    val inputData = MemoryStream[Long]
    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .where('value >= current_timestamp().cast("long") - 10L)
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(aggregated, Complete)(
      StartStream(Trigger.ProcessingTime("10 seconds"), triggerClock = clock),

      // advance clock to 10 seconds, all keys retained
      //提前时钟到10秒,所有的Key保留
      AddData(inputData, 0L, 5L, 5L, 10L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((0L, 1), (5L, 2), (10L, 1)),

      // advance clock to 20 seconds, should retain keys >= 10
      //提前时钟到20秒,应该保留键> = 10
      AddData(inputData, 15L, 15L, 20L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((10L, 1), (15L, 2), (20L, 1)),

      // advance clock to 30 seconds, should retain keys >= 20
      //提前时钟30秒，应保留Key> = 20
      AddData(inputData, 0L, 85L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((20L, 1), (85L, 1)),

      // bounce stream and ensure correct batch timestamp is used
      //反弹流并确保使用正确的批处理时间戳
      // i.e., we don't take it from the clock, which is at 90 seconds.
      StopStream,
      AssertOnQuery { q => // clear the sink
        q.sink.asInstanceOf[MemorySink].clear()
        q.batchCommitLog.purge(3)
        // advance by a minute i.e., 90 seconds total
        //总共提前一分钟，即90秒
        clock.advance(60 * 1000L)
        true
      },
      StartStream(Trigger.ProcessingTime("10 seconds"), triggerClock = clock),
      // The commit log blown, causing the last batch to re-run
      //提交日志被炸毁，导致最后一批重新运行
      CheckLastBatch((20L, 1), (85L, 1)),
      AssertOnQuery { q =>
        clock.getTimeMillis() == 90000L
      },

      // advance clock to 100 seconds, should retain keys >= 90
      //提前时钟到100秒，应该保留key> = 90
      AddData(inputData, 85L, 90L, 100L, 105L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((90L, 1), (100L, 1), (105L, 1))
    )
  }
  //通过current_date修剪结果，完成模式
  ignore("prune results by current_date, complete mode") {
    import testImplicits._
    val clock = new StreamManualClock
    val tz = TimeZone.getDefault.getID
    val inputData = MemoryStream[Long]
    val aggregated =
      inputData.toDF()
        .select(to_utc_timestamp(from_unixtime('value * DateTimeUtils.SECONDS_PER_DAY), tz))
        .toDF("value")
        .groupBy($"value")
        .agg(count("*"))
        .where($"value".cast("date") >= date_sub(current_date(), 10))
        .select(($"value".cast("long") / DateTimeUtils.SECONDS_PER_DAY).cast("long"), $"count(1)")
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    testStream(aggregated, Complete)(
      StartStream(Trigger.ProcessingTime("10 day"), triggerClock = clock),
      // advance clock to 10 days, should retain all keys
      //提前时间到10天，应该保留所有的key
      AddData(inputData, 0L, 5L, 5L, 10L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((0L, 1), (5L, 2), (10L, 1)),
      // advance clock to 20 days, should retain keys >= 10
      //提前时间20天，应保留key> = 10
      AddData(inputData, 15L, 15L, 20L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((10L, 1), (15L, 2), (20L, 1)),
      // advance clock to 30 days, should retain keys >= 20
      //提前30天的时间，应保留key> = 20
      AddData(inputData, 85L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((20L, 1), (85L, 1)),

      // bounce stream and ensure correct batch timestamp is used
      //反弹流并确保使用正确的批处理时间戳
      // i.e., we don't take it from the clock, which is at 90 days.
      //我们不会把它从90天的时钟中拿走
      StopStream,
      AssertOnQuery { q => // clear the sink
        q.sink.asInstanceOf[MemorySink].clear()
        q.batchCommitLog.purge(3)
        // advance by 60 days i.e., 90 days total
        //提前60天,即总共90天
        clock.advance(DateTimeUtils.MILLIS_PER_DAY * 60)
        true
      },
      StartStream(Trigger.ProcessingTime("10 day"), triggerClock = clock),
      // Commit log blown, causing a re-run of the last batch
      //提交日志,导致重新运行最后一批
      CheckLastBatch((20L, 1), (85L, 1)),

      // advance clock to 100 days, should retain keys >= 90
      //提前时间到100天，应该保留key> = 90
      AddData(inputData, 85L, 90L, 100L, 105L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((90L, 1), (100L, 1), (105L, 1))
    )
  }
  //不要将流式查询中的批量聚合转换为流式处理
  test("SPARK-19690: do not convert batch aggregation in streaming query to streaming") {
    val streamInput = MemoryStream[Int]
    val batchDF = Seq(1, 2, 3, 4, 5)
        .toDF("value")
        .withColumn("parity", 'value % 2)
        .groupBy('parity)
        .agg(count("*") as 'joinValue)
    val joinDF = streamInput
        .toDF()
        .join(batchDF, 'value === 'parity)

    // make sure we're planning an aggregate in the first place
    //确保我们正在计划一个聚合首先
    assert(batchDF.queryExecution.optimizedPlan match { case _: Aggregate => true })
    //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
    //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
    //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
    testStream(joinDF, Append)(
      AddData(streamInput, 0, 1, 2, 3),
      CheckLastBatch((0, 0, 2), (1, 1, 3)),
      AddData(streamInput, 0, 1, 2, 3),
      CheckLastBatch((0, 0, 2), (1, 1, 3)))
  }
}
