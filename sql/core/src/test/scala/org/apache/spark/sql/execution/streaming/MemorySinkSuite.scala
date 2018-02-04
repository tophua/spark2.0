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

import scala.language.implicitConversions

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

class MemorySinkSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }
  //直接在追加输出模式下添加数据
  test("directly add data in Append output mode") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    //"Output"是写入到外部存储的写方式,
    //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
    val sink = new MemorySink(schema, OutputMode.Append)

    // Before adding data, check output
    //在添加数据之前,请检查输出
    assert(sink.latestBatchId === None)
    checkAnswer(sink.latestBatchData, Seq.empty)
    checkAnswer(sink.allData, Seq.empty)

    // Add batch 0 and check outputs
    //添加批次0并检查输出
    sink.addBatch(0, 1 to 3)
    assert(sink.latestBatchId === Some(0))
    checkAnswer(sink.latestBatchData, 1 to 3)
    checkAnswer(sink.allData, 1 to 3)

    // Add batch 1 and check outputs
    //添加批次1并检查输出
    sink.addBatch(1, 4 to 6)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    //新数据应附加到旧数据
    checkAnswer(sink.allData, 1 to 6)     // new data should get appended to old data

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    //用不同的数据重新添加批次1,不应添加,输出不应改变
    sink.addBatch(1, 7 to 9)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6)

    // Add batch 2 and check outputs
    sink.addBatch(2, 7 to 9)
    assert(sink.latestBatchId === Some(2))
    checkAnswer(sink.latestBatchData, 7 to 9)
    checkAnswer(sink.allData, 1 to 9)
  }
  //在更新输出模式下直接添加数据
  //Output"是写入到外部存储的写方式,
  test("directly add data in Update output mode") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    val sink = new MemorySink(schema, OutputMode.Update)

    // Before adding data, check output
    //在添加数据之前,请检查输出
    assert(sink.latestBatchId === None)
    checkAnswer(sink.latestBatchData, Seq.empty)
    checkAnswer(sink.allData, Seq.empty)

    // Add batch 0 and check outputs
    //添加批次0并检查输出
    sink.addBatch(0, 1 to 3)
    assert(sink.latestBatchId === Some(0))
    checkAnswer(sink.latestBatchData, 1 to 3)
    checkAnswer(sink.allData, 1 to 3)

    // Add batch 1 and check outputs
    sink.addBatch(1, 4 to 6)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6) // new data should get appended to old data

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    //用不同的数据重新添加批次1，不应添加，输出不应改变
    sink.addBatch(1, 7 to 9)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 1 to 6)

    // Add batch 2 and check outputs
    //添加批次2并检查输出
    sink.addBatch(2, 7 to 9)
    assert(sink.latestBatchId === Some(2))
    checkAnswer(sink.latestBatchData, 7 to 9)
    checkAnswer(sink.allData, 1 to 9)
  }
  //直接在完整输出模式下添加数据
  test("directly add data in Complete output mode") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
    val sink = new MemorySink(schema, OutputMode.Complete)

    // Before adding data, check output
    //在添加数据之前，请检查输出
    assert(sink.latestBatchId === None)
    checkAnswer(sink.latestBatchData, Seq.empty)
    checkAnswer(sink.allData, Seq.empty)

    // Add batch 0 and check outputs
    //添加批次0并检查输出
    sink.addBatch(0, 1 to 3)
    assert(sink.latestBatchId === Some(0))
    checkAnswer(sink.latestBatchData, 1 to 3)
    checkAnswer(sink.allData, 1 to 3)

    // Add batch 1 and check outputs
    //添加批次1并检查输出
    sink.addBatch(1, 4 to 6)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 4 to 6)     // new data should replace old data

    // Re-add batch 1 with different data, should not be added and outputs should not be changed
    //用不同的数据重新添加批次1，不应添加，输出不应改变
    sink.addBatch(1, 7 to 9)
    assert(sink.latestBatchId === Some(1))
    checkAnswer(sink.latestBatchData, 4 to 6)
    checkAnswer(sink.allData, 4 to 6)

    // Add batch 2 and check outputs
    sink.addBatch(2, 7 to 9)
    assert(sink.latestBatchId === Some(2))
    checkAnswer(sink.latestBatchData, 7 to 9)
    checkAnswer(sink.allData, 7 to 9)
  }

  //以附加输出模式注册为表格
  test("registering as a table in Append output mode") {
    val input = MemoryStream[Int]
    val query = input.toDF().writeStream
      // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
      // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
      .format("memory")
      //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
      .outputMode("append")
      .queryName("memStream")
      .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3)

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3, 4, 5, 6)

    query.stop()
  }
  //在完整输出模式下注册为表
  //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
  test("registering as a table in Complete output mode") {
    val input = MemoryStream[Int]
    val query = input.toDF()
      .groupBy("value")
      .count()
      .writeStream
      // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
      // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
      .format("memory")
      //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
      .outputMode("complete")
      .queryName("memStream")
      .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDatasetUnorderly(
      spark.table("memStream").as[(Int, Long)],
      (1, 1L), (2, 1L), (3, 1L))

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDatasetUnorderly(
      spark.table("memStream").as[(Int, Long)],
      (1, 1L), (2, 1L), (3, 1L), (4, 1L), (5, 1L), (6, 1L))

    query.stop()
  }
  //以更新输出模式注册为表
  test("registering as a table in Update output mode") {
    val input = MemoryStream[Int]
    val query = input.toDF().writeStream
      // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
      // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
      .format("memory")
      .outputMode("update")
      .queryName("memStream")
      .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()

    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3)

    input.addData(4, 5, 6)
    query.processAllAvailable()
    checkDataset(
      spark.table("memStream").as[Int],
      1, 2, 3, 4, 5, 6)

    query.stop()
  }
  //内存计划统计
  test("MemoryPlan statistics") {
    implicit val schema = new StructType().add(new StructField("value", IntegerType))
    //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
    val sink = new MemorySink(schema, OutputMode.Append)
    val plan = new MemoryPlan(sink)

    // Before adding data, check output
    //在添加数据之前,请检查输出
    checkAnswer(sink.allData, Seq.empty)
    assert(plan.stats.sizeInBytes === 0)

    sink.addBatch(0, 1 to 3)
    plan.invalidateStatsCache()
    assert(plan.stats.sizeInBytes === 12)

    sink.addBatch(1, 4 to 6)
    plan.invalidateStatsCache()
    assert(plan.stats.sizeInBytes === 24)
  }
  //压力测试
  ignore("stress test") {
    // Ignore the stress test as it takes several minutes to run
    //忽略压力测试,因为它需要几分钟才能运行
    (0 until 1000).foreach { _ =>
      val input = MemoryStream[Int]
      val query = input.toDF().writeStream
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        .queryName("memStream")
        .start()
      input.addData(1, 2, 3)
      query.processAllAvailable()

      checkDataset(
        spark.table("memStream").as[Int],
        1, 2, 3)

      input.addData(4, 5, 6)
      query.processAllAvailable()
      checkDataset(
        spark.table("memStream").as[Int],
        1, 2, 3, 4, 5, 6)

      query.stop()
    }
  }
  //未指定名称时发生错误
  test("error when no name is specified") {
    val error = intercept[AnalysisException] {
      val input = MemoryStream[Int]
      val query = input.toDF().writeStream
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
          .format("memory")
          .start()
    }

    assert(error.message contains "queryName must be specified")
  }
  //如果试图恢复特定的检查点错误
  test("error if attempting to resume specific checkpoint") {
    val location = Utils.createTempDir(namePrefix = "steaming.checkpoint").getCanonicalPath

    val input = MemoryStream[Int]
    val query = input.toDF().writeStream
      // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
      // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        .queryName("memStream")
        .option("checkpointLocation", location)
        .start()
    input.addData(1, 2, 3)
    query.processAllAvailable()
    query.stop()

    intercept[AnalysisException] {
      input.toDF().writeStream
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        .queryName("memStream")
        .option("checkpointLocation", location)
        .start()
    }
  }

  private def checkAnswer(rows: Seq[Row], expected: Seq[Int])(implicit schema: StructType): Unit = {
    checkAnswer(
      sqlContext.createDataFrame(sparkContext.makeRDD(rows), schema),
      intsToDF(expected)(schema))
  }

  private implicit def intsToDF(seq: Seq[Int])(implicit schema: StructType): DataFrame = {
    require(schema.fields.size === 1)
    sqlContext.createDataset(seq).toDF(schema.fieldNames.head)
  }
}
