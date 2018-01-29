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

import java.util.concurrent.CountDownLatch

import scala.concurrent.Future
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.BlockingSource
import org.apache.spark.util.Utils

class StreamingQueryManagerSuite extends StreamTest with BeforeAndAfter {

  import AwaitTerminationTester._
  import testImplicits._

  override val streamingTimeout = 20.seconds

  before {
    assert(spark.streams.active.isEmpty)
    spark.streams.resetTerminated()
  }

  after {
    assert(spark.streams.active.isEmpty)
    spark.streams.resetTerminated()
  }

  testQuietly("listing") {
    val (m1, ds1) = makeDataset
    val (m2, ds2) = makeDataset
    val (m3, ds3) = makeDataset

    withQueriesOn(ds1, ds2, ds3) { queries =>
      require(queries.size === 3)
      assert(spark.streams.active.toSet === queries.toSet)
      val (q1, q2, q3) = (queries(0), queries(1), queries(2))

      assert(spark.streams.get(q1.id).eq(q1))
      assert(spark.streams.get(q2.id).eq(q2))
      assert(spark.streams.get(q3.id).eq(q3))
      assert(spark.streams.get(java.util.UUID.randomUUID()) === null) // non-existent id
      q1.stop()

      assert(spark.streams.active.toSet === Set(q2, q3))
      assert(spark.streams.get(q1.id) === null)
      assert(spark.streams.get(q2.id).eq(q2))

      m2.addData(0)   // q2 should terminate with error

      eventually(Timeout(streamingTimeout)) {
        require(!q2.isActive)
        require(q2.exception.isDefined)
        assert(spark.streams.get(q2.id) === null)
        assert(spark.streams.active.toSet === Set(q3))
      }
    }
  }
  //等待任何终止没有超时和resetTerminated
  testQuietly("awaitAnyTermination without timeout and resetTerminated") {
    val datasets = Seq.fill(5)(makeDataset._2)
    withQueriesOn(datasets: _*) { queries =>
      require(queries.size === datasets.size)
      assert(spark.streams.active.toSet === queries.toSet)

      // awaitAnyTermination should be blocking
      testAwaitAnyTermination(ExpectBlocked)

      // Stop a query asynchronously and see if it is reported through awaitAnyTermination
      //异步停止查询，查看是否通过awaitAnyTermination报告
      val q1 = stopRandomQueryAsync(stopAfter = 100 milliseconds, withError = false)
      testAwaitAnyTermination(ExpectNotBlocked)
      //在prev awaitAnyTerm返回的时候应该是不活动
      require(!q1.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should be non-blocking
      //所有后续调用awaitAnyTermination应该是非阻塞
      testAwaitAnyTermination(ExpectNotBlocked)

      // Resetting termination should make awaitAnyTermination() blocking again
      //重置终止应该使awaitAnyTermination（）再次被阻塞
      spark.streams.resetTerminated()
      testAwaitAnyTermination(ExpectBlocked)

      // Terminate a query asynchronously with exception and see awaitAnyTermination throws
      // the exception
      //用异常异步终止查询,请参阅awaitAnyTermination抛出异常
      val q2 = stopRandomQueryAsync(100 milliseconds, withError = true)
      testAwaitAnyTermination(ExpectException[SparkException])
      require(!q2.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should throw the exception
      //所有后续调用awaitAnyTermination应该抛出异常
      testAwaitAnyTermination(ExpectException[SparkException])

      // Resetting termination should make awaitAnyTermination() blocking again
      spark.streams.resetTerminated()
      testAwaitAnyTermination(ExpectBlocked)

      // Terminate multiple queries, one with failure and see whether awaitAnyTermination throws
      // the exception
      //终止多个查询,其中一个查询失败,查看awaitAnyTermination是否抛出异常
      val q3 = stopRandomQueryAsync(10 milliseconds, withError = false)
      testAwaitAnyTermination(ExpectNotBlocked)
      require(!q3.isActive)
      val q4 = stopRandomQueryAsync(10 milliseconds, withError = true)
      eventually(Timeout(streamingTimeout)) { require(!q4.isActive) }
      // After q4 terminates with exception, awaitAnyTerm should start throwing exception
      testAwaitAnyTermination(ExpectException[SparkException])
    }
  }
  //awaitAnyTermination超时并重置已终止
  testQuietly("awaitAnyTermination with timeout and resetTerminated") {
    val datasets = Seq.fill(6)(makeDataset._2)
    withQueriesOn(datasets: _*) { queries =>
      require(queries.size === datasets.size)
      assert(spark.streams.active.toSet === queries.toSet)

      // awaitAnyTermination should be blocking or non-blocking depending on timeout values
      //awaitAnyTermination应根据超时值进行阻塞或非阻塞
      testAwaitAnyTermination(
        ExpectBlocked,
        awaitTimeout = 4 seconds,
        expectedReturnedValue = false,
        testBehaviorFor = 2 seconds)

      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 50 milliseconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1 second)

      // Stop a query asynchronously within timeout and awaitAnyTerm should be unblocked
      //在超时内异步停止查询，并且awaitAnyTerm应该被解除阻塞
      val q1 = stopRandomQueryAsync(stopAfter = 100 milliseconds, withError = false)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 2 seconds,
        expectedReturnedValue = true,
        testBehaviorFor = 4 seconds)
      //在prev awaitAnyTerm返回的时候应该是不活动的
      require(!q1.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should be non-blocking even if timeout is high
      //所有后续的awaitAnyTermination调用都应该是非阻塞的，即使超时时间很长
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 4 seconds, expectedReturnedValue = true)

      // Resetting termination should make awaitAnyTermination() blocking again
      spark.streams.resetTerminated()
      testAwaitAnyTermination(
        ExpectBlocked,
        awaitTimeout = 4 seconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1 second)

      // Terminate a query asynchronously with exception within timeout, awaitAnyTermination should
      // throws the exception
      //在超时时间内异常终止查询,awaitAnyTermination应抛出异常
      val q2 = stopRandomQueryAsync(100 milliseconds, withError = true)
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 4 seconds,
        testBehaviorFor = 6 seconds)
      require(!q2.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should throw the exception
      //所有后续调用awaitAnyTermination应该抛出异常
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 2 seconds,
        testBehaviorFor = 4 seconds)

      // Terminate a query asynchronously outside the timeout, awaitAnyTerm should be blocked
      //在超时之外异步终止查询，awaitAnyTerm应该被阻塞
      spark.streams.resetTerminated()
      val q3 = stopRandomQueryAsync(2 seconds, withError = true)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 100 milliseconds,
        expectedReturnedValue = false,
        testBehaviorFor = 4 seconds)

      // After that query is stopped, awaitAnyTerm should throw exception
      //在查询停止之后,awaitAnyTerm应该抛出异常
      eventually(Timeout(streamingTimeout)) { require(!q3.isActive) } // wait for query to stop
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 100 milliseconds,
        testBehaviorFor = 4 seconds)


      // Terminate multiple queries, one with failure and see whether awaitAnyTermination throws
      // the exception
      //终止多个查询，其中一个查询失败,查看awaitAnyTermination是否抛出异常
      spark.streams.resetTerminated()

      val q4 = stopRandomQueryAsync(10 milliseconds, withError = false)
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 2 seconds, expectedReturnedValue = true)
      require(!q4.isActive)
      val q5 = stopRandomQueryAsync(10 milliseconds, withError = true)
      eventually(Timeout(streamingTimeout)) { require(!q5.isActive) }
      // After q5 terminates with exception, awaitAnyTerm should start throwing exception
      testAwaitAnyTermination(ExpectException[SparkException], awaitTimeout = 2 seconds)
    }
  }
  //源代码解析不应该阻塞主线程
  test("SPARK-18811: Source resolution should not block main thread") {
    failAfter(streamingTimeout) {
      BlockingSource.latch = new CountDownLatch(1)
      withTempDir { tempDir =>
        // if source resolution was happening on the main thread, it would block the start call,
        // now it should only be blocking the stream execution thread
        //如果在主线程上发生源代码解析,它会阻塞启动调用,现在只能阻塞流执行线程
        val sq = spark.readStream
          .format("org.apache.spark.sql.streaming.util.BlockingSource")
          .load()
          .writeStream
          .format("org.apache.spark.sql.streaming.util.BlockingSource")
          .option("checkpointLocation", tempDir.toString)
          .start()
        eventually(Timeout(streamingTimeout)) {
          assert(sq.status.message.contains("Initializing sources"))
        }
        BlockingSource.latch.countDown()
        sq.stop()
      }
    }
  }

  /** Run a body of code by defining a query on each dataset
    * 通过在每个数据集上定义一个查询运行一段代码*/
  private def withQueriesOn(datasets: Dataset[_]*)(body: Seq[StreamingQuery] => Unit): Unit = {
    failAfter(streamingTimeout) {
      val queries = withClue("Error starting queries") {
        datasets.zipWithIndex.map { case (ds, i) =>
          var query: StreamingQuery = null
          try {
            val df = ds.toDF
            val metadataRoot =
              Utils.createTempDir(namePrefix = "streaming.checkpoint").getCanonicalPath
            query =
              df.writeStream
                .format("memory")
                .queryName(s"query$i")
                .option("checkpointLocation", metadataRoot)
                .outputMode("append")
                .start()
          } catch {
            case NonFatal(e) =>
              if (query != null) query.stop()
              throw e
          }
          query
        }
      }
      try {
        body(queries)
      } finally {
        queries.foreach(_.stop())
      }
    }
  }

  /** Test the behavior of awaitAnyTermination
    * 测试awaitAnyTermination的行为*/
  private def testAwaitAnyTermination(
      expectedBehavior: ExpectedBehavior,
      expectedReturnedValue: Boolean = false,
      awaitTimeout: Span = null,
      testBehaviorFor: Span = 4 seconds
    ): Unit = {

    def awaitTermFunc(): Unit = {
      if (awaitTimeout != null && awaitTimeout.toMillis > 0) {
        val returnedValue = spark.streams.awaitAnyTermination(awaitTimeout.toMillis)
        assert(returnedValue === expectedReturnedValue, "Returned value does not match expected")
      } else {
        spark.streams.awaitAnyTermination()
      }
    }

    AwaitTerminationTester.test(expectedBehavior, awaitTermFunc, testBehaviorFor)
  }

  /** Stop a random active query either with `stop()` or with an error
    * 使用stop（）或者错误来停止一个随机的活动查询*/
  private def stopRandomQueryAsync(stopAfter: Span, withError: Boolean): StreamingQuery = {

    import scala.concurrent.ExecutionContext.Implicits.global

    val activeQueries = spark.streams.active
    val queryToStop = activeQueries(Random.nextInt(activeQueries.length))
    Future {
      Thread.sleep(stopAfter.toMillis)
      if (withError) {
        logDebug(s"Terminating query ${queryToStop.name} with error")
        queryToStop.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan.collect {
          case StreamingExecutionRelation(source, _) =>
            source.asInstanceOf[MemoryStream[Int]].addData(0)
        }
      } else {
        logDebug(s"Stopping query ${queryToStop.name}")
        queryToStop.stop()
      }
    }
    queryToStop
  }

  private def makeDataset: (MemoryStream[Int], Dataset[Int]) = {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS.map(6 / _)
    (inputData, mapped)
  }
}
