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

package org.apache.spark.sql.execution.ui

import java.util.Properties

import org.json4s.jackson.JsonMethods._
import org.mockito.Mockito.mock

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{AccumulatorMetadata, JsonProtocol, LongAccumulator}


class SQLListenerSuite extends SparkFunSuite with SharedSQLContext with JsonTestUtils {
  import testImplicits._
  import org.apache.spark.AccumulatorSuite.makeInfo

  private def createTestDataFrame: DataFrame = {
    Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1")
  }

  private def createProperties(executionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
    properties
  }

  private def createStageInfo(stageId: Int, attemptId: Int): StageInfo = new StageInfo(
    stageId = stageId,
    attemptId = attemptId,
    // The following fields are not used in tests
    //以下字段不用于测试
    name = "",
    numTasks = 0,
    rddInfos = Nil,
    parentIds = Nil,
    details = ""
  )

  private def createTaskInfo(taskId: Int, attemptNumber: Int): TaskInfo = new TaskInfo(
    taskId = taskId,
    attemptNumber = attemptNumber,
    // The following fields are not used in tests
    //以下字段不用于测试
    index = 0,
    launchTime = 0,
    executorId = "",
    host = "",
    taskLocality = null,
    speculative = false
  )

  private def createTaskMetrics(accumulatorUpdates: Map[Long, Long]): TaskMetrics = {
    val metrics = TaskMetrics.empty
    accumulatorUpdates.foreach { case (id, update) =>
      val acc = new LongAccumulator
      acc.metadata = AccumulatorMetadata(id, Some(""), true)
      acc.add(update)
      metrics.registerAccumulator(acc)
    }
    metrics
  }
  //基本
  test("basic") {
    def checkAnswer(actual: Map[Long, String], expected: Map[Long, Long]): Unit = {
      assert(actual.size == expected.size)
      expected.foreach { e =>
        // The values in actual can be SQL metrics meaning that they contain additional formatting
        // when converted to string. Verify that they start with the expected value.
        // TODO: this is brittle. There is no requirement that the actual string needs to start
        // with the accumulator value.
        assert(actual.contains(e._1))
        val v = actual.get(e._1).get.trim
        assert(v.startsWith(e._2.toString))
      }
    }

    val listener = new SQLListener(spark.sparkContext.conf)
    val executionId = 0
    val df = createTestDataFrame
    val accumulatorIds =
      SparkPlanGraph(SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan))
        .allNodes.flatMap(_.metrics.map(_.accumulatorId))
    // Assume all accumulators are long
    //假设所有的累加器都很长
    var accumulatorValue = 0L
    val accumulatorUpdates = accumulatorIds.map { id =>
      accumulatorValue += 1L
      (id, accumulatorValue)
    }.toMap

    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))

    val executionUIData = listener.executionIdToData(0)

    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq(
        createStageInfo(0, 0),
        createStageInfo(1, 0)
      ),
      createProperties(executionId)))
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 0)))

    assert(listener.getExecutionMetrics(0).isEmpty)

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo)),
      (1L, 0, 0, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo))
    )))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    // Driver accumulator updates don't belong to this execution should be filtered and no
    // exception will be thrown.
    //驱动程序累加器更新不属于这个执行过程,应该被过滤并且不会抛出异常
    listener.onOtherEvent(SparkListenerDriverAccumUpdates(0, Seq((999L, 2L))))
    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 0, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo)),
      (1L, 0, 0,
        createTaskMetrics(accumulatorUpdates.mapValues(_ * 2)).accumulators().map(makeInfo))
    )))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 3))

    // Retrying a stage should reset the metrics
    //重试阶段应重置指标
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(0, 1)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      (0L, 0, 1, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo)),
      (1L, 0, 1, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo))
    )))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    // Ignore the task end for the first attempt
    //忽略第一次尝试的任务结束
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 100))))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 2))

    // Finish two tasks
    //完成两个任务
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 2))))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 0,
      stageAttemptId = 1,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 3))))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 5))

    // Summit a new stage
    //高峰一个新阶段
    listener.onStageSubmitted(SparkListenerStageSubmitted(createStageInfo(1, 0)))

    listener.onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate("", Seq(
      // (task id, stage id, stage attempt, accum updates)
      //（任务ID，ID尝试阶段，阶段，累积更新）
      (0L, 1, 0, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo)),
      (1L, 1, 0, createTaskMetrics(accumulatorUpdates).accumulators().map(makeInfo))
    )))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 7))

    // Finish two tasks 完成任务
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(0, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 3))))
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId = 1,
      stageAttemptId = 0,
      taskType = "",
      reason = null,
      createTaskInfo(1, 0),
      createTaskMetrics(accumulatorUpdates.mapValues(_ * 3))))

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 11))

    assert(executionUIData.runningJobs === Seq(0))
    assert(executionUIData.succeededJobs.isEmpty)
    assert(executionUIData.failedJobs.isEmpty)

    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))

    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs === Seq(0))
    assert(executionUIData.failedJobs.isEmpty)

    checkAnswer(listener.getExecutionMetrics(0), accumulatorUpdates.mapValues(_ * 11))
  }
  //onexecutionend之前发生的onjobend（jobsucceeded）
  test("onExecutionEnd happens before onJobEnd(JobSucceeded)") {
    val listener = new SQLListener(spark.sparkContext.conf)
    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    val executionUIData = listener.executionIdToData(0)
    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs === Seq(0))
    assert(executionUIData.failedJobs.isEmpty)
  }
  //onexecutionend之前发生的多onjobend（jobsucceeded）
  test("onExecutionEnd happens before multiple onJobEnd(JobSucceeded)s") {
    val listener = new SQLListener(spark.sparkContext.conf)
    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onJobEnd(SparkListenerJobEnd(
        jobId = 0,
        time = System.currentTimeMillis(),
        JobSucceeded
    ))

    listener.onJobStart(SparkListenerJobStart(
      jobId = 1,
      time = System.currentTimeMillis(),
      stageInfos = Nil,
      createProperties(executionId)))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 1,
      time = System.currentTimeMillis(),
      JobSucceeded
    ))

    val executionUIData = listener.executionIdToData(0)
    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs.sorted === Seq(0, 1))
    assert(executionUIData.failedJobs.isEmpty)
  }
  //onexecutionend之前发生的onjobend（jobfailed）
  test("onExecutionEnd happens before onJobEnd(JobFailed)") {
    val listener = new SQLListener(spark.sparkContext.conf)
    val executionId = 0
    val df = createTestDataFrame
    listener.onOtherEvent(SparkListenerSQLExecutionStart(
      executionId,
      "test",
      "test",
      df.queryExecution.toString,
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan),
      System.currentTimeMillis()))
    listener.onJobStart(SparkListenerJobStart(
      jobId = 0,
      time = System.currentTimeMillis(),
      stageInfos = Seq.empty,
      createProperties(executionId)))
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(
      executionId, System.currentTimeMillis()))
    listener.onJobEnd(SparkListenerJobEnd(
      jobId = 0,
      time = System.currentTimeMillis(),
      JobFailed(new RuntimeException("Oops"))
    ))

    val executionUIData = listener.executionIdToData(0)
    assert(executionUIData.runningJobs.isEmpty)
    assert(executionUIData.succeededJobs.isEmpty)
    assert(executionUIData.failedJobs === Seq(0))
  }
  //运行非SQL作业时无内存泄漏
  test("SPARK-11126: no memory leak when running non SQL jobs") {
    val previousStageNumber = spark.sharedState.listener.stageIdToStageMetrics.size
    spark.sparkContext.parallelize(1 to 10).foreach(i => ())
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    // listener should ignore the non SQL stage
    //侦听器应忽略非SQL阶段
    assert(spark.sharedState.listener.stageIdToStageMetrics.size == previousStageNumber)

    spark.sparkContext.parallelize(1 to 10).toDF().foreach(i => ())
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    // listener should save the SQL stage
    //侦听器应保存SQL阶段
    assert(spark.sharedState.listener.stageIdToStageMetrics.size == previousStageNumber + 1)
  }
  //历史侦听器只跟踪SQL度量
  test("SPARK-13055: history listener only tracks SQL metrics") {
    val listener = new SQLHistoryListener(sparkContext.conf, mock(classOf[SparkUI]))
    // We need to post other events for the listener to track our accumulators.
    //我们需要为侦听器发布其他事件来跟踪累加器
    // These are largely just boilerplate unrelated to what we're trying to test.
    //这些都在很大程度上只是样板与我们想测试
    val df = createTestDataFrame
    val executionStart = SparkListenerSQLExecutionStart(
      0, "", "", "", SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan), 0)
    val stageInfo = createStageInfo(0, 0)
    val jobStart = SparkListenerJobStart(0, 0, Seq(stageInfo), createProperties(0))
    val stageSubmitted = SparkListenerStageSubmitted(stageInfo)
    // This task has both accumulators that are SQL metrics and accumulators that are not.
    //这个任务有两个累加器，它们是SQL度量和累加器
    // The listener should only track the ones that are actually SQL metrics.
    //侦听器应该只跟踪那些实际上是SQL度量的数据
    val sqlMetric = SQLMetrics.createMetric(sparkContext, "beach umbrella")
    val nonSqlMetric = sparkContext.longAccumulator("baseball")
    val sqlMetricInfo = sqlMetric.toInfo(Some(sqlMetric.value), None)
    val nonSqlMetricInfo = nonSqlMetric.toInfo(Some(nonSqlMetric.value), None)
    val taskInfo = createTaskInfo(0, 0)
    taskInfo.setAccumulables(List(sqlMetricInfo, nonSqlMetricInfo))
    val taskEnd = SparkListenerTaskEnd(0, 0, "just-a-task", null, taskInfo, null)
    listener.onOtherEvent(executionStart)
    listener.onJobStart(jobStart)
    listener.onStageSubmitted(stageSubmitted)
    // Before SPARK-13055, this throws ClassCastException because the history listener would
    // assume that the accumulator value is of type Long, but this may not be true for
    // accumulators that are not SQL metrics.
    listener.onTaskEnd(taskEnd)
    val trackedAccums = listener.stageIdToStageMetrics.values.flatMap { stageMetrics =>
      stageMetrics.taskIdToMetricUpdates.values.flatMap(_.accumulatorUpdates)
    }
    // Listener tracks only SQL metrics, not other accumulators
    //侦听器只跟踪SQL度量值,而不跟踪其他累加器
    assert(trackedAccums.size === 1)
    assert(trackedAccums.head === ((sqlMetricInfo.id, sqlMetricInfo.update.get)))
  }
  //驱动端SQL度量
  test("driver side SQL metrics") {
    val listener = new SQLListener(spark.sparkContext.conf)
    val expectedAccumValue = 12345
    val physicalPlan = MyPlan(sqlContext.sparkContext, expectedAccumValue)
    sqlContext.sparkContext.addSparkListener(listener)
    val dummyQueryExecution = new QueryExecution(spark, LocalRelation()) {
      override lazy val sparkPlan = physicalPlan
      override lazy val executedPlan = physicalPlan
    }
    SQLExecution.withNewExecutionId(spark, dummyQueryExecution) {
      physicalPlan.execute().collect()
    }

    def waitTillExecutionFinished(): Unit = {
      while (listener.getCompletedExecutions.isEmpty) {
        Thread.sleep(100)
      }
    }
    waitTillExecutionFinished()

    val driverUpdates = listener.getCompletedExecutions.head.driverAccumUpdates
    assert(driverUpdates.size == 1)
    assert(driverUpdates(physicalPlan.longMetric("dummy").id) == expectedAccumValue)
  }
  //
  test("roundtripping SparkListenerDriverAccumUpdates through JsonProtocol (SPARK-18462)") {
    val event = SparkListenerDriverAccumUpdates(1L, Seq((2L, 3L)))
    val json = JsonProtocol.sparkEventToJson(event)
    assertValidDataInJson(json,
      parse("""
        |{
        |  "Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
        |  "executionId": 1,
        |  "accumUpdates": [[2,3]]
        |}
      """.stripMargin))
    JsonProtocol.sparkEventFromJson(json) match {
      case SparkListenerDriverAccumUpdates(executionId, accums) =>
        assert(executionId == 1L)
        accums.foreach { case (a, b) =>
          assert(a == 2L)
          assert(b == 3L)
        }
    }

    // Test a case where the numbers in the JSON can only fit in longs:
    //测试JSON中的数字只能满足long的情况
    val longJson = parse(
      """
        |{
        |  "Event": "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates",
        |  "executionId": 4294967294,
        |  "accumUpdates": [[4294967294,3]]
        |}
      """.stripMargin)
    JsonProtocol.sparkEventFromJson(longJson) match {
      case SparkListenerDriverAccumUpdates(executionId, accums) =>
        assert(executionId == 4294967294L)
        accums.foreach { case (a, b) =>
          assert(a == 4294967294L)
          assert(b == 3L)
        }
    }
  }

}


/**
 * A dummy [[org.apache.spark.sql.execution.SparkPlan]] that updates a [[SQLMetrics]]
 * on the driver.
 */
private case class MyPlan(sc: SparkContext, expectedValue: Long) extends LeafExecNode {
  override def sparkContext: SparkContext = sc
  override def output: Seq[Attribute] = Seq()

  override val metrics: Map[String, SQLMetric] = Map(
    "dummy" -> SQLMetrics.createMetric(sc, "dummy"))

  override def doExecute(): RDD[InternalRow] = {
    longMetric("dummy") += expectedValue

    SQLMetrics.postDriverMetricUpdates(
      sc,
      sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY),
      metrics.values.toSeq)
    sc.emptyRDD
  }
}


class SQLListenerMemoryLeakSuite extends SparkFunSuite {
  //没有内存泄漏
  test("no memory leak") {
    quietly {
      val conf = new SparkConf()
        .setMaster("local")
        .setAppName("test")
        //不要重试要快速运行此测试的任务
        .set(config.MAX_TASK_FAILURES, 1) // Don't retry the tasks to run this test quickly
        //将其设置为50即可快速运行此测试
        .set("spark.sql.ui.retainedExecutions", "50") // Set it to 50 to run this test quickly
      withSpark(new SparkContext(conf)) { sc =>
        SparkSession.sqlListener.set(null)
        val spark = new SparkSession(sc)
        import spark.implicits._
        // Run 100 successful executions and 100 failed executions.
        //执行100次成功处决和100次失败处决
        // Each execution only has one job and one stage.
        //每个执行只有一个作业和一个阶段
        for (i <- 0 until 100) {
          val df = Seq(
            (1, 1),
            (2, 2)
          ).toDF()
          df.collect()
          try {
            df.foreach(_ => throw new RuntimeException("Oops"))
          } catch {
            //这是一份失败的工作
            case e: SparkException => // This is expected for a failed job
          }
        }
        sc.listenerBus.waitUntilEmpty(10000)
        assert(spark.sharedState.listener.getCompletedExecutions.size <= 50)
        assert(spark.sharedState.listener.getFailedExecutions.size <= 50)
        // 50 for successful executions and 50 for failed executions
        //成功处决50人,失败处决者50人
        assert(spark.sharedState.listener.executionIdToData.size <= 100)
        assert(spark.sharedState.listener.jobIdToExecutionId.size <= 100)
        assert(spark.sharedState.listener.stageIdToStageMetrics.size <= 100)
      }
    }
  }
}
