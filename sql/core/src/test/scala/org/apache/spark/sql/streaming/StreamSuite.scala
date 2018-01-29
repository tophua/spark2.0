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

import java.io.{File, InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeoutException, TimeUnit}

import scala.reflect.ClassTag
import scala.util.control.ControlThrowable

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

class StreamSuite extends StreamTest {

  import testImplicits._
  //映射与恢复
  test("map with recovery") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped)(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswer(2, 3, 4),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7))
  }

  test("join") {
    // Make a table and ensure it will be broadcast.
    //制作一张表,并确保它将被广播。
    val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

    // Join the input stream with a table.
    //用表格加入输入流
    val inputData = MemoryStream[Int]
    val joined = inputData.toDS().toDF().join(smallTable, $"value" === $"number")

    testStream(joined)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two")),
      AddData(inputData, 4),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two"), Row(4, 4, "four")))
  }

  //解释加入
  test("explain join") {
    // Make a table and ensure it will be broadcast.
    //制作一张表,并确保它将被广播。
    val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

    // Join the input stream with a table.
    //用表格加入输入流
    val inputData = MemoryStream[Int]
    val joined = inputData.toDF().join(smallTable, smallTable("number") === $"value")

    val outputStream = new java.io.ByteArrayOutputStream()
    Console.withOut(outputStream) {
      joined.explain()
    }
    assert(outputStream.toString.contains("StreamingRelation"))
  }
  //结合一个流与自己
  test("SPARK-20432: union one stream with itself") {
    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load().select("a")
    val unioned = df.union(df)
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        val query =
          unioned
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
        try {
          query.processAllAvailable()
          val outputDf = spark.read.parquet(outputDir.getAbsolutePath).as[Long]
          checkDatasetUnorderly[Long](outputDf, (0L to 10L).union((0L to 10L)).toArray: _*)
        } finally {
          query.stop()
        }
      }
    }
  }
  //联合两个流
  test("union two streams") {
    val inputData1 = MemoryStream[Int]
    val inputData2 = MemoryStream[Int]

    val unioned = inputData1.toDS().union(inputData2.toDS())

    testStream(unioned)(
      AddData(inputData1, 1, 3, 5),
      CheckAnswer(1, 3, 5),
      AddData(inputData2, 2, 4, 6),
      CheckAnswer(1, 2, 3, 4, 5, 6),
      StopStream,
      AddData(inputData1, 7),
      StartStream(),
      AddData(inputData2, 8),
      CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8))
  }
  //sql查询
  test("sql queries") {
    val inputData = MemoryStream[Int]
    inputData.toDF().createOrReplaceTempView("stream")
    val evens = sql("SELECT * FROM stream WHERE value % 2 = 0")

    testStream(evens)(
      AddData(inputData, 1, 2, 3, 4),
      CheckAnswer(2, 4))
  }
  //DataFrame重用
  test("DataFrame reuse") {
    def assertDF(df: DataFrame) {
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val query = df.writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
          try {
            query.processAllAvailable()
            val outputDf = spark.read.parquet(outputDir.getAbsolutePath).as[Long]
            checkDataset[Long](outputDf, (0L to 10L).toArray: _*)
          } finally {
            query.stop()
          }
        }
      }
    }

    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
    assertDF(df)
    assertDF(df)
  }
  //在同一个流式查询中,只能将一个StreamingRelation转换为一个StreamingExecutionRelation
  test("Within the same streaming query, one StreamingRelation should only be transformed to one " +
    "StreamingExecutionRelation") {
    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
    var query: StreamExecution = null
    try {
      query =
        df.union(df)
          .writeStream
          .format("memory")
          .queryName("memory")
          .start()
          .asInstanceOf[StreamingQueryWrapper]
          .streamingQuery
      query.awaitInitialization(streamingTimeout.toMillis)
      val executionRelations =
        query
          .logicalPlan
          .collect { case ser: StreamingExecutionRelation => ser }
      assert(executionRelations.size === 2)
      assert(executionRelations.distinct.size === 1)
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }
  //不受支持的查询
  test("unsupported queries") {
    val streamInput = MemoryStream[Int]
    val batchInput = Seq(1, 2, 3).toDS()

    def assertError(expectedMsgs: Seq[String])(body: => Unit): Unit = {
      val e = intercept[AnalysisException] {
        body
      }
      expectedMsgs.foreach { s => assert(e.getMessage.contains(s)) }
    }

    // Running streaming plan as a batch query
    //作为批量查询运行流计划
    assertError("start" :: Nil) {
      streamInput.toDS.map { i => i }.count()
    }

    // Running non-streaming plan with as a streaming query
    //为流式查询运行非流式计划
    assertError("without streaming sources" :: "start" :: Nil) {
      val ds = batchInput.map { i => i }
      testStream(ds)()
    }

    // Running streaming plan that cannot be incrementalized
    //运行无法进行增量化的流式计划
    assertError("not supported" :: "streaming" :: Nil) {
      val ds = streamInput.toDS.map { i => i }.sort()
      testStream(ds)()
    }
  }
  //尽可能减少批量生产和执行之间
  test("minimize delay between batch construction and execution") {

    // For each batch, we would retrieve new data's offsets and log them before we run the execution
    //对于每个批次，我们将检索新数据的偏移量并在执行之前将其记录下来
    // This checks whether the key of the offset log is the expected batch id
    //这将检查偏移日志的密钥是否是预期的批次标识
    def CheckOffsetLogLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.offsetLog.getLatest().get._1 == expectedId,
        s"offsetLog's latest should be $expectedId")

    // Check the latest batchid in the commit log
    //检查提交日志中的最新的批次ID
    def CheckCommitLogLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.batchCommitLog.getLatest().get._1 == expectedId,
        s"commitLog's latest should be $expectedId")

    // Ensure that there has not been an incremental execution after restart
    //确保重新启动后没有增量执行
    def CheckNoIncrementalExecutionCurrentBatchId(): AssertOnQuery =
      AssertOnQuery(_.lastExecution == null, s"lastExecution not expected to run")

    // For each batch, we would log the state change during the execution
    //对于每个批次，我们都会记录执行期间的状态更改
    // This checks whether the key of the state change log is the expected batch id
    //这将检查状态更改日志的密钥是否是预期的批处理标识
    def CheckIncrementalExecutionCurrentBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.lastExecution.asInstanceOf[IncrementalExecution].currentBatchId == expectedId,
        s"lastExecution's currentBatchId should be $expectedId")

    // For each batch, we would log the sink change after the execution
    //对于每个批次，我们会在执行后记录接收器更改
    // This checks whether the key of the sink change log is the expected batch id
    //这将检查接收器更改日志的密钥是否是预期的批次ID
    def CheckSinkLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.sink.asInstanceOf[MemorySink].latestBatchId.get == expectedId,
        s"sink's lastBatchId should be $expectedId")

    val inputData = MemoryStream[Int]
    testStream(inputData.toDS())(
      StartStream(ProcessingTime("10 seconds"), new StreamManualClock),

      /* -- batch 0 ----------------------- */
      // Add some data in batch 0
      //批量添加一些数据0
      AddData(inputData, 1, 2, 3),
      AdvanceManualClock(10 * 1000), // 10 seconds

      /* -- batch 1 ----------------------- */
      // Check the results of batch 0
      //检查批次0的结果
      CheckAnswer(1, 2, 3),
      CheckIncrementalExecutionCurrentBatchId(0),
      CheckCommitLogLatestBatchId(0),
      CheckOffsetLogLatestBatchId(0),
      CheckSinkLatestBatchId(0),
      // Add some data in batch 1
      //在第1批中添加一些数据
      AddData(inputData, 4, 5, 6),
      AdvanceManualClock(10 * 1000),

      /* -- batch _ ----------------------- */
      // Check the results of batch 1
      //检查批次1的结果
      CheckAnswer(1, 2, 3, 4, 5, 6),
      CheckIncrementalExecutionCurrentBatchId(1),
      CheckCommitLogLatestBatchId(1),
      CheckOffsetLogLatestBatchId(1),
      CheckSinkLatestBatchId(1),

      AdvanceManualClock(10 * 1000),
      AdvanceManualClock(10 * 1000),
      AdvanceManualClock(10 * 1000),

      /* -- batch __ ---------------------- */
      // Check the results of batch 1 again; this is to make sure that, when there's no new data,
      //再次检查批次1的结果; 这是为了确保在没有新的数据时，
      // the currentId does not get logged (e.g. as 2) even if the clock has advanced many times
      //即使时钟已经提前多次，currentId也不会被记录（例如2）
      CheckAnswer(1, 2, 3, 4, 5, 6),
      CheckIncrementalExecutionCurrentBatchId(1),
      CheckCommitLogLatestBatchId(1),
      CheckOffsetLogLatestBatchId(1),
      CheckSinkLatestBatchId(1),

      /* Stop then restart the Stream 停止然后重新启动流 */
      StopStream,
      StartStream(ProcessingTime("10 seconds"), new StreamManualClock(60 * 1000)),

      /* -- batch 1 no rerun ----------------- */
      // batch 1 would not re-run because the latest batch id logged in commit log is 1
      //批处理1不会重新运行，因为提交日志中记录的最新批处理标识为1
      AdvanceManualClock(10 * 1000),
      CheckNoIncrementalExecutionCurrentBatchId(),

      /* -- batch 2 ----------------------- */
      // Check the results of batch 1 检查批次1的结果
      CheckAnswer(1, 2, 3, 4, 5, 6),
      CheckCommitLogLatestBatchId(1),
      CheckOffsetLogLatestBatchId(1),
      CheckSinkLatestBatchId(1),
      // Add some data in batch 2 在批次2中添加一些数据
      AddData(inputData, 7, 8, 9),
      AdvanceManualClock(10 * 1000),

      /* -- batch 3 ----------------------- */
      // Check the results of batch 2  Check the results of batch 2
      //检查批次2的结果检查批次2的结果
      CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9),
      CheckIncrementalExecutionCurrentBatchId(2),
      CheckCommitLogLatestBatchId(2),
      CheckOffsetLogLatestBatchId(2),
      CheckSinkLatestBatchId(2))
  }
  //插入额外的策略
  test("insert an extraStrategy") {
    try {
      spark.experimental.extraStrategies = TestStrategy :: Nil

      val inputData = MemoryStream[(String, Int)]
      val df = inputData.toDS().map(_._1).toDF("a")

      testStream(df)(
        AddData(inputData, ("so slow", 1)),
        CheckAnswer("so fast"))
    } finally {
      spark.experimental.extraStrategies = Nil
    }
  }
  //处理从流线程抛出的致命错误
  testQuietly("handle fatal errors thrown from the stream thread") {
    for (e <- Seq(
      new VirtualMachineError {},
      new ThreadDeath,
      new LinkageError,
      new ControlThrowable {}
    )) {
      val source = new Source {
        override def getOffset: Option[Offset] = {
          throw e
        }

        override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
          throw e
        }

        override def schema: StructType = StructType(Array(StructField("value", IntegerType)))

        override def stop(): Unit = {}
      }
      val df = Dataset[Int](
        sqlContext.sparkSession,
        StreamingExecutionRelation(source, sqlContext.sparkSession))
      testStream(df)(
        // `ExpectFailure(isFatalError = true)` verifies two things:
        // - Fatal errors can be propagated to `StreamingQuery.exception` and
        //   `StreamingQuery.awaitTermination` like non fatal errors.
        // - Fatal errors can be caught by UncaughtExceptionHandler.
        ExpectFailure(isFatalError = true)(ClassTag(e.getClass))
      )
    }
  }
  //输出模式API在Scala
  test("output mode API in Scala") {
    assert(OutputMode.Append === InternalOutputModes.Append)
    assert(OutputMode.Complete === InternalOutputModes.Complete)
    assert(OutputMode.Update === InternalOutputModes.Update)
  }
  //说明
  test("explain") {
    val inputData = MemoryStream[String]
    val df = inputData.toDS().map(_ + "foo").groupBy("value").agg(count("*"))

    // Test `df.explain`
    val explain = ExplainCommand(df.queryExecution.logical, extended = false)
    val explainString =
      spark.sessionState
        .executePlan(explain)
        .executedPlan
        .executeCollect()
        .map(_.getString(0))
        .mkString("\n")
    assert(explainString.contains("StateStoreRestore"))
    assert(explainString.contains("StreamingRelation"))
    assert(!explainString.contains("LocalTableScan"))

    // Test StreamingQuery.display 测试StreamingQuery.display
    val q = df.writeStream.queryName("memory_explain").outputMode("complete").format("memory")
      .start()
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
    try {
      assert("No physical plan. Waiting for data." === q.explainInternal(false))
      assert("No physical plan. Waiting for data." === q.explainInternal(true))

      inputData.addData("abc")
      q.processAllAvailable()

      val explainWithoutExtended = q.explainInternal(false)
      // `extended = false` only displays the physical plan.
      //`extended = false`只显示物理计划
      assert("LocalRelation".r.findAllMatchIn(explainWithoutExtended).size === 0)
      assert("LocalTableScan".r.findAllMatchIn(explainWithoutExtended).size === 1)
      // Use "StateStoreRestore" to verify that it does output a streaming physical plan
      //使用“StateStoreRestore”来验证它是否输出流式物理计划
      assert(explainWithoutExtended.contains("StateStoreRestore"))

      val explainWithExtended = q.explainInternal(true)
      // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
      // plan.
      assert("LocalRelation".r.findAllMatchIn(explainWithExtended).size === 3)
      assert("LocalTableScan".r.findAllMatchIn(explainWithExtended).size === 1)
      // Use "StateStoreRestore" to verify that it does output a streaming physical plan
      //使用“StateStoreRestore”来验证它是否输出流式物理计划
      assert(explainWithExtended.contains("StateStoreRestore"))
    } finally {
      q.stop()
    }
  }
  //dropDuplicates不应该使用相同的id创建表达式
  test("SPARK-19065: dropDuplicates should not create expressions using the same id") {
    withTempPath { testPath =>
      val data = Seq((1, 2), (2, 3), (3, 4))
      data.toDS.write.mode("overwrite").json(testPath.getCanonicalPath)
      val schema = spark.read.json(testPath.getCanonicalPath).schema
      val query = spark
        .readStream
        .schema(schema)
        .json(testPath.getCanonicalPath)
        .dropDuplicates("_1")
        .writeStream
        .format("memory")
        .queryName("testquery")
        .outputMode("append")
        .start()
      try {
        query.processAllAvailable()
        if (query.exception.isDefined) {
          throw query.exception.get
        }
      } finally {
        query.stop()
      }
    }
  }
  //当流线程中断时处理IOException（Hadoop 2.8之前的版本）
  test("handle IOException when the streaming thread is interrupted (pre Hadoop 2.8)") {
    // This test uses a fake source to throw the same IOException as pre Hadoop 2.8 when the
    // streaming thread is interrupted. We should handle it properly by not failing the query.
    //当流式处理线程中断时,此测试使用假来源来抛出与Hadoop 2.8之前相同的IOException, 我们应该正确处理,不要使查询失败。
    ThrowingIOExceptionLikeHadoop12074.createSourceLatch = new CountDownLatch(1)
    val query = spark
      .readStream
      .format(classOf[ThrowingIOExceptionLikeHadoop12074].getName)
      .load()
      .writeStream
      .format("console")
      .start()
    assert(ThrowingIOExceptionLikeHadoop12074.createSourceLatch
      .await(streamingTimeout.toMillis, TimeUnit.MILLISECONDS),
      "ThrowingIOExceptionLikeHadoop12074.createSource wasn't called before timeout")
    query.stop()
    assert(query.exception.isEmpty)
  }
  //当流线程中断时处理InterruptedIOException（Hadoop 2.8+）
  test("handle InterruptedIOException when the streaming thread is interrupted (Hadoop 2.8+)") {
    // This test uses a fake source to throw the same InterruptedIOException as Hadoop 2.8+ when the
    // streaming thread is interrupted. We should handle it properly by not failing the query.
    //当流式处理线程被中断时,这个测试使用一个假来源来抛出与Hadoop 2.8+
    // 相同的InterruptedIOException,我们应该正确处理,不要使查询失败。
    ThrowingInterruptedIOException.createSourceLatch = new CountDownLatch(1)
    val query = spark
      .readStream
      .format(classOf[ThrowingInterruptedIOException].getName)
      .load()
      .writeStream
      .format("console")
      .start()
    assert(ThrowingInterruptedIOException.createSourceLatch
      .await(streamingTimeout.toMillis, TimeUnit.MILLISECONDS),
      "ThrowingInterruptedIOException.createSource wasn't called before timeout")
    query.stop()
    assert(query.exception.isEmpty)
  }
  //流式聚合以及分区数量的变化
  test("SPARK-19873: streaming aggregation with change in number of partitions") {
    val inputData = MemoryStream[(Int, Int)]
    val agg = inputData.toDS().groupBy("_1").count()

    testStream(agg, OutputMode.Complete())(
      AddData(inputData, (1, 0), (2, 0)),
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "2")),
      CheckAnswer((1, 1), (2, 1)),
      StopStream,
      AddData(inputData, (3, 0), (2, 0)),
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "5")),
      CheckAnswer((1, 1), (2, 2), (3, 1)),
      StopStream,
      AddData(inputData, (3, 0), (1, 0)),
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "1")),
      CheckAnswer((1, 2), (2, 2), (3, 2)))
  }
  //从Spark v2.1检查点恢复
  testQuietly("recover from a Spark v2.1 checkpoint") {
    var inputData: MemoryStream[Int] = null
    var query: DataStreamWriter[Row] = null

    def prepareMemoryStream(): Unit = {
      inputData = MemoryStream[Int]
      inputData.addData(1, 2, 3, 4)
      inputData.addData(3, 4, 5, 6)
      inputData.addData(5, 6, 7, 8)

      query = inputData
        .toDF()
        .groupBy($"value")
        .agg(count("*"))
        .writeStream
        .outputMode("complete")
        .format("memory")
    }

    // Get an existing checkpoint generated by Spark v2.1.
    //获取由Spark v2.1生成的现有检查点
    // v2.1 does not record # shuffle partitions in the offset metadata.
    //v2.1不会在偏移量元数据中记录＃个shuffle分区
    val resourceUri =
      this.getClass.getResource("/structured-streaming/checkpoint-version-2.1.0").toURI
    val checkpointDir = new File(resourceUri)

    // 1 - Test if recovery from the checkpoint is successful.
    //1 - 测试从检查点恢复是否成功
    prepareMemoryStream()
    val dir1 = Utils.createTempDir().getCanonicalFile // not using withTempDir {}, makes test flaky
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    //将检查点复制到临时目录以防止更改原始目录
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    //不这样做会导致测试通过第一次运行,但失败后续运行
    FileUtils.copyDirectory(checkpointDir, dir1)
    // Checkpoint data was generated by a query with 10 shuffle partitions.
    //检查点数据由具有10个随机分区的查询生成
    // In order to test reading from the checkpoint, the checkpoint must have two or more batches,
    //为了测试检查点的读数，检查点必须有两个或更多个批次
    // since the last batch may be rerun.
    //因为最后一批可能会重新运行
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      var streamingQuery: StreamingQuery = null
      try {
        streamingQuery =
          query.queryName("counts").option("checkpointLocation", dir1.getCanonicalPath).start()
        streamingQuery.processAllAvailable()
        inputData.addData(9)
        streamingQuery.processAllAvailable()

        QueryTest.checkAnswer(spark.table("counts").toDF(),
          Row("1", 1) :: Row("2", 1) :: Row("3", 2) :: Row("4", 2) ::
          Row("5", 2) :: Row("6", 2) :: Row("7", 1) :: Row("8", 1) :: Row("9", 1) :: Nil)
      } finally {
        if (streamingQuery ne null) {
          streamingQuery.stop()
        }
      }
    }

    // 2 - Check recovery with wrong num shuffle partitions
    //使用错误的数字随机分区检查恢复
    prepareMemoryStream()
    val dir2 = Utils.createTempDir().getCanonicalFile
    FileUtils.copyDirectory(checkpointDir, dir2)
    // Since the number of partitions is greater than 10, should throw exception.
    //由于分区数大于10,应该抛出异常
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "15") {
      var streamingQuery: StreamingQuery = null
      try {
        intercept[StreamingQueryException] {
          streamingQuery =
            query.queryName("badQuery").option("checkpointLocation", dir2.getCanonicalPath).start()
          streamingQuery.processAllAvailable()
        }
      } finally {
        if (streamingQuery ne null) {
          streamingQuery.stop()
        }
      }
    }
  }
  //在查询上取消相关的工作
  test("calling stop() on a query cancels related jobs") {
    val input = MemoryStream[Int]
    val query = input
      .toDS()
      .map { i =>
        while (!org.apache.spark.TaskContext.get().isInterrupted()) {
          // keep looping till interrupted by query.stop()
          //保持循环直到被query.stop（）中断
          Thread.sleep(100)
        }
        i
      }
      .writeStream
      .format("console")
      .start()

    input.addData(1)
    // wait for jobs to start 等待工作开始
    eventually(timeout(streamingTimeout)) {
      assert(sparkContext.statusTracker.getActiveJobIds().nonEmpty)
    }

    query.stop()
    // make sure jobs are stopped 确保工作停止
    eventually(timeout(streamingTimeout)) {
      assert(sparkContext.statusTracker.getActiveJobIds().isEmpty)
    }
  }
  //批处理ID在作业说明中正确更新
  test("batch id is updated correctly in the job description") {
    val queryName = "memStream"
    @volatile var jobDescription: String = null
    def assertDescContainsQueryNameAnd(batch: Integer): Unit = {
      // wait for listener event to be processed 等待侦听器事件被处理
      spark.sparkContext.listenerBus.waitUntilEmpty(streamingTimeout.toMillis)
      assert(jobDescription.contains(queryName) && jobDescription.contains(s"batch = $batch"))
    }

    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobDescription = jobStart.properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      }
    })

    val input = MemoryStream[Int]
    val query = input
      .toDS()
      .map(_ + 1)
      .writeStream
      .format("memory")
      .queryName(queryName)
      .start()

    input.addData(1)
    query.processAllAvailable()
    assertDescContainsQueryNameAnd(batch = 0)
    input.addData(2, 3)
    query.processAllAvailable()
    assertDescContainsQueryNameAnd(batch = 1)
    input.addData(4)
    query.processAllAvailable()
    assertDescContainsQueryNameAnd(batch = 2)
    query.stop()
  }
  //应该解决检查点路径
  test("should resolve the checkpoint path") {
    withTempDir { dir =>
      val checkpointLocation = dir.getCanonicalPath
      assert(!checkpointLocation.startsWith("file:/"))
      val query = MemoryStream[Int].toDF
        .writeStream
        .option("checkpointLocation", checkpointLocation)
        .format("console")
        .start()
      try {
        val resolvedCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot
        assert(resolvedCheckpointDir.startsWith("file:/"))
      } finally {
        query.stop()
      }
    }
  }
  //指定自定义状态存储提供者
  testQuietly("specify custom state store provider") {
    val providerClassName = classOf[TestStateStoreProvider].getCanonicalName
    withSQLConf("spark.sql.streaming.stateStore.providerClass" -> providerClassName) {
      val input = MemoryStream[Int]
      val df = input.toDS().groupBy().count()
      val query = df.writeStream.outputMode("complete").format("memory").queryName("name").start()
      input.addData(1, 2, 3)
      val e = intercept[Exception] {
        query.awaitTermination()
      }

      assert(e.getMessage.contains(providerClassName))
      assert(e.getMessage.contains("instantiated"))
    }
  }
  //自定义状态存储提供者从偏移日志中读取
  testQuietly("custom state store provider read from offset log") {
    val input = MemoryStream[Int]
    val df = input.toDS().groupBy().count()
    val providerConf1 = "spark.sql.streaming.stateStore.providerClass" ->
      "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
    val providerConf2 = "spark.sql.streaming.stateStore.providerClass" ->
      classOf[TestStateStoreProvider].getCanonicalName

    def runQuery(queryName: String, checkpointLoc: String): Unit = {
      val query = df.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName(queryName)
        .option("checkpointLocation", checkpointLoc)
        .start()
      input.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()
    }

    withTempDir { dir =>
      val checkpointLoc1 = new File(dir, "1").getCanonicalPath
      withSQLConf(providerConf1) {
        runQuery("query1", checkpointLoc1)  // generate checkpoints
      }

      val checkpointLoc2 = new File(dir, "2").getCanonicalPath
      withSQLConf(providerConf2) {
        // Verify new query will use new provider that throw error on loading
        //验证新的查询将使用加载时出错的新提供程序
        intercept[Exception] {
          runQuery("query2", checkpointLoc2)
        }

        // Verify old query from checkpoint will still use old provider
        //验证来自检查点的旧查询仍将使用旧提供者
        runQuery("query1", checkpointLoc1)
      }
    }
  }

  for (e <- Seq(
    new InterruptedException,
    new InterruptedIOException,
    new ClosedByInterruptException,
    new UncheckedIOException("test", new ClosedByInterruptException),
    new ExecutionException("test", new InterruptedException),
    new UncheckedExecutionException("test", new InterruptedException))) {
    test(s"view ${e.getClass.getSimpleName} as a normal query stop") {
      ThrowingExceptionInCreateSource.createSourceLatch = new CountDownLatch(1)
      ThrowingExceptionInCreateSource.exception = e
      val query = spark
        .readStream
        .format(classOf[ThrowingExceptionInCreateSource].getName)
        .load()
        .writeStream
        .format("console")
        .start()
      assert(ThrowingExceptionInCreateSource.createSourceLatch
        .await(streamingTimeout.toMillis, TimeUnit.MILLISECONDS),
        "ThrowingExceptionInCreateSource.createSource wasn't called before timeout")
      query.stop()
      assert(query.exception.isEmpty)
    }
  }
}

abstract class FakeSource extends StreamSourceProvider {
  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = ("fakeSource", fakeSchema)
}

/** A fake StreamSourceProvider that creates a fake Source that cannot be reused.
  * 一个假的StreamSourceProvider，创建一个不能被重用的假源*/
class FakeDefaultSource extends FakeSource {

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    // Create a fake Source that emits 0 to 10.
    //创建一个假的来源，发出0到10。
    new Source {
      private var offset = -1L

      override def schema: StructType = StructType(StructField("a", IntegerType) :: Nil)

      override def getOffset: Option[Offset] = {
        if (offset >= 10) {
          None
        } else {
          offset += 1
          Some(LongOffset(offset))
        }
      }

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        val startOffset = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L) + 1
        val ds = new Dataset[java.lang.Long](
          spark.sparkSession,
          Range(
            startOffset,
            end.asInstanceOf[LongOffset].offset + 1,
            1,
            Some(spark.sparkSession.sparkContext.defaultParallelism),
            isStreaming = true),
          Encoders.LONG)
        ds.toDF("a")
      }

      override def stop() {}
    }
  }
}

/** A fake source that throws the same IOException like pre Hadoop 2.8 when it's interrupted.
  * 一个假的源，当它被打断时抛出与Hadoop 2.8之前相同的IOException异常*/
class ThrowingIOExceptionLikeHadoop12074 extends FakeSource {
  import ThrowingIOExceptionLikeHadoop12074._

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case ie: InterruptedException =>
        throw new IOException(ie.toString)
    }
  }
}

object ThrowingIOExceptionLikeHadoop12074 {
  /**
   * A latch to allow the user to wait until `ThrowingIOExceptionLikeHadoop12074.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
}

/** A fake source that throws InterruptedIOException like Hadoop 2.8+ when it's interrupted.
  * 当Hadoop 2.8+中断时，会引发InterruptedIOException。 */
class ThrowingInterruptedIOException extends FakeSource {
  import ThrowingInterruptedIOException._

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case ie: InterruptedException =>
        val iie = new InterruptedIOException(ie.toString)
        iie.initCause(ie)
        throw iie
    }
  }
}

object ThrowingInterruptedIOException {
  /**
   * A latch to allow the user to wait until `ThrowingInterruptedIOException.createSource` is
   * called.一个闩锁允许用户等待，直到调用ThrowingInterruptedIOException.createSource。
   */
  @volatile var createSourceLatch: CountDownLatch = null
}

class TestStateStoreProvider extends StateStoreProvider {

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int],
      storeConfs: StateStoreConf,
      hadoopConf: Configuration): Unit = {
    throw new Exception("Successfully instantiated")
  }

  override def stateStoreId: StateStoreId = null

  override def close(): Unit = { }

  override def getStore(version: Long): StateStore = null
}

/** A fake source that throws `ThrowingExceptionInCreateSource.exception` in `createSource`
  * 在`createSource`中抛出`ThrowingExceptionInCreateSource.exception`的假源*/
class ThrowingExceptionInCreateSource extends FakeSource {

  override def createSource(
    spark: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source = {
    ThrowingExceptionInCreateSource.createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case _: InterruptedException =>
        throw ThrowingExceptionInCreateSource.exception
    }
  }
}

object ThrowingExceptionInCreateSource {
  /**
   * A latch to allow the user to wait until `ThrowingExceptionInCreateSource.createSource` is
   * called.
    * 一个锁允许用户等待直到调用ThrowingExceptionInCreateSource.createSource
   */
  @volatile var createSourceLatch: CountDownLatch = null
  @volatile var exception: Exception = null
}
