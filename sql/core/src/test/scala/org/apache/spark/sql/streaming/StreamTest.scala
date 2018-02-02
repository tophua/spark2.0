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

import java.lang.Thread.UncaughtExceptionHandler

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.{Assertions, BeforeAndAfterAll}
import org.scalatest.concurrent.{Eventually, Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{Dataset, Encoder, QueryTest, Row}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * A framework for implementing tests for streaming queries and sources.
  * 一个流式查询和来源的测试框架
 *
 * A test consists of a set of steps (expressed as a `StreamAction`) that are executed in order,
  * 一个测试包括一系列的步骤（表示为“StreamAction”）
 * blocking as necessary to let the stream catch up.  For example, the following adds some data to
 * a stream, blocking until it can verify that the correct values are eventually produced.
  * 必要时阻止流式缓存,例如,以下内容将一些数据添加到流中,直到它可以验证最终生成正确的值为止,
 *
 * {{{
 *  val inputData = MemoryStream[Int]
 *  val mapped = inputData.toDS().map(_ + 1)
 *
 *  testStream(mapped)(
 *    AddData(inputData, 1, 2, 3),
 *    CheckAnswer(2, 3, 4))
 * }}}
 *
 * Note that while we do sleep to allow the other thread to progress without spinning,
  * 请注意,虽然我们睡觉,让其他线程进展而不旋转,
 * `StreamAction` checks should not depend on the amount of time spent sleeping.  Instead they
 * should check the actual progress of the stream before verifying the required test condition.
 *`StreamAction`检查不应该依赖于花费的时间,相反,他们应该在验证所需的测试条件之前检查流的实际进度。
  *
 * Currently it is assumed that all streaming queries will eventually complete in 10 seconds to
 * avoid hanging forever in the case of failures. However, individual suites can change this
 * by overriding `streamingTimeout`.
  * 目前假定所有流式查询最终将在10秒内完成,以避免在失败的情况下永远悬挂,但是,
  * 单个套件可以通过覆盖`streamingTimeout`来改变它
 */
trait StreamTest extends QueryTest with SharedSQLContext with TimeLimits with BeforeAndAfterAll {

  implicit val defaultSignaler: Signaler = ThreadSignaler
  override def afterAll(): Unit = {
    super.afterAll()
    //停止状态存储维护线程并卸载存储提供程序
    StateStore.stop() // stop the state store maintenance thread and unload store providers
  }

  /** How long to wait for an active stream to catch up when checking a result.
    * 在检查结果时等待活动流多长时间？*/
  val streamingTimeout = 10.seconds

  /** A trait for actions that can be performed while testing a streaming DataFrame.
    * 测试流式DataFrame时可以执行的操作的特征*/
  trait StreamAction

  /** A trait to mark actions that require the stream to be actively running.
    * 用于标识需要流运行的动作的特性*/
  trait StreamMustBeRunning

  /**
   * Adds the given data to the stream. Subsequent check answers will block until this data has
   * been processed.
    * 将给定的数据添加到流中,随后的检查答案将被阻塞,直到数据被处理为止
   */
  object AddData {
    def apply[A](source: MemoryStream[A], data: A*): AddDataMemory[A] ={
      //println(source.id+"===="+data.mkString(","))
      AddDataMemory(source, data)
    }

  }

  /** A trait that can be extended when testing a source.
    * 测试源代码时可以扩展的特性*/
  trait AddData extends StreamAction {
    /**
     * Called to adding the data to a source. It should find the source to add data to from
     * the active query, and then return the source object the data was added, as well as the
     * offset of added data.
      * 调用将数据添加到源,它应该找到从活动查询添加数据的源,然后返回数据添加的源对象,以及添加数据的偏移量。
     */
    def addData(query: Option[StreamExecution]): (Source, Offset)
  }

  /** A trait that can be extended when testing a source.
    * 测试源代码时可以扩展的特性 */
  trait ExternalAction extends StreamAction {
    def runAction(): Unit
  }

  case class AddDataMemory[A](source: MemoryStream[A], data: Seq[A]) extends AddData {
    override def toString: String = s"AddData to $source: ${data.mkString(",")}"

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      (source, source.addData(data))
    }
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
    * 检查以确保存储在接收器中的当前数据与“预期答案”相匹配
   * This operation automatically blocks until all added data has been processed.
    * 该操作将自动阻塞,直到处理完所有添加的数据
   */
  object CheckAnswer {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = false,
        isSorted = false)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, false, false)
  }

  /**
   * Checks to make sure that the current data stored in the sink matches the `expectedAnswer`.
    * 检查以确保存储在接收器中的当前数据与“预期答案”相匹配
   * This operation automatically blocks until all added data has been processed.
    * 该操作将自动阻塞,直到处理完所有添加的数据。
   */
  object CheckLastBatch {
    def apply[A : Encoder](data: A*): CheckAnswerRows = {
      apply(isSorted = false, data: _*)
    }

    def apply[A: Encoder](isSorted: Boolean, data: A*): CheckAnswerRows = {
      val encoder = encoderFor[A]
      val toExternalRow = RowEncoder(encoder.schema).resolveAndBind()
      CheckAnswerRows(
        data.map(d => toExternalRow.fromRow(encoder.toRow(d))),
        lastOnly = true,
        isSorted = isSorted)
    }

    def apply(rows: Row*): CheckAnswerRows = CheckAnswerRows(rows, true, false)
  }

  case class CheckAnswerRows(expectedAnswer: Seq[Row], lastOnly: Boolean, isSorted: Boolean)
      extends StreamAction with StreamMustBeRunning {
    override def toString: String = s"$operatorName: ${expectedAnswer.mkString(",")}"
    private def operatorName = if (lastOnly) "CheckLastBatch" else "CheckAnswer"
  }

  /** Stops the stream. It must currently be running.
    * 停止流,它目前必须在运行*/
  case object StopStream extends StreamAction with StreamMustBeRunning

  /** Starts the stream, resuming if data has already been processed. It must not be running.
    * 启动流,如果数据已经处理,则恢复,它不能运行*/
  case class StartStream(
       //根据处理时间间隔周期性地运行查询的触发器策略,如果“间隔”为0，查询将尽可能快地运行
      trigger: Trigger = Trigger.ProcessingTime(0),
      triggerClock: Clock = new SystemClock,
      additionalConfs: Map[String, String] = Map.empty)
    extends StreamAction

  /** Advance the trigger clock's time manually.
    * 手动提前触发时钟的时间*/
  case class AdvanceManualClock(timeToAdd: Long) extends StreamAction

  /**
   * Signals that a failure is expected and should not kill the test.
   * 表示预计失败,不应该杀死测试
   * @param isFatalError if this is a fatal error. If so, the error should also be caught by
   *                     UncaughtExceptionHandler.
    *                     如果这是一个致命的错误,如果是这样,错误也应该被UncaughtExceptionHandler捕获
   * @param assertFailure a function to verify the error. 一个函数来验证错误
   */
  case class ExpectFailure[T <: Throwable : ClassTag](
      assertFailure: Throwable => Unit = _ => {},
      isFatalError: Boolean = false) extends StreamAction {
    val causeClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    override def toString(): String =
      s"ExpectFailure[${causeClass.getName}, isFatalError: $isFatalError]"
  }

  /** Assert that a body is true  断言身体是真的*/
  class Assert(condition: => Boolean, val message: String = "") extends StreamAction {
    def run(): Unit = { Assertions.assert(condition) }
    override def toString: String = s"Assert(<condition>, $message)"
  }

  object Assert {
    def apply(condition: => Boolean, message: String = ""): Assert = new Assert(condition, message)
    def apply(message: String)(body: => Unit): Assert = new Assert( { body; true }, message)
    def apply(body: => Unit): Assert = new Assert( { body; true }, "")
  }

  /** Assert that a condition on the active query is true  断言在活动查询条件是真实的*/
  class AssertOnQuery(val condition: StreamExecution => Boolean, val message: String)
    extends StreamAction {
    override def toString: String = s"AssertOnQuery(<condition>, $message)"
  }

  object AssertOnQuery {
    def apply(condition: StreamExecution => Boolean, message: String = ""): AssertOnQuery = {
      new AssertOnQuery(condition, message)
    }

    def apply(message: String)(condition: StreamExecution => Boolean): AssertOnQuery = {
      new AssertOnQuery(condition, message)
    }
  }

  /** Execute arbitrary code  执行任意代码*/
  object Execute {
    def apply(func: StreamExecution => Any): AssertOnQuery =
      AssertOnQuery(query => { func(query); true })
  }

  /**
   * Executes the specified actions on the given streaming DataFrame and provides helpful
   * error messages in the case of failures or incorrect answers.
   * 在给定的数据流DataFrame上执行指定的操作,并在出现故障或不正确的答案时提供有用的错误消息。
   * Note that if the stream is not explicitly started before an action that requires it to be
   * running then it will be automatically started before performing any other actions.
    * 请注意,如果流在需要运行的操作之前未明确启动,那么将在执行任何其他操作之前自动启动它
   */
  def testStream(
      _stream: Dataset[_],
      //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
      //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
      //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
      outputMode: OutputMode = OutputMode.Append)(actions: StreamAction*): Unit = synchronized {
    import org.apache.spark.sql.streaming.util.StreamManualClock

    // `synchronized` is added to prevent the user from calling multiple `testStream`s concurrently
    //`synchronized 同步`添加防止用户调用多个` teststream `的同时
    // because this method assumes there is only one active query in its `StreamingQueryListener`
    // and it may not work correctly when multiple `testStream`s run concurrently.
    //因为此方法假定只有一个活动查询其` StreamingQueryListener `,
    //它可能无法正常工作在多个` teststream `的并发运行
    val stream = _stream.toDF()
    //在DF中使用会话，而不是默认会话
    val sparkSession = stream.sparkSession  // use the session in DF, not the default session
    var pos = 0
    var currentStream: StreamExecution = null
    var lastStream: StreamExecution = null
    //源索引 - >偏移等待
    val awaiting = new mutable.HashMap[Int, Offset]() // source index -> offset to wait for
    //将结果存储在内存中的接收器,这个[[Sink]]主要用于单元测试,不提供耐久性
    //"Output"是写入到外部存储的写方式
    val sink = new MemorySink(stream.schema, outputMode)
    val resetConfValues = mutable.Map[String, Option[String]]()

    @volatile //不稳定
    var streamThreadDeathCause: Throwable = null
    // Set UncaughtExceptionHandler in `onQueryStarted` so that we can ensure catching fatal errors
    // during query initialization.
    //在`onQueryStarted`中设置UncaughtExceptionHandler,这样我们可以确保在查询初始化时捕获致命错误。
    val listener = new StreamingQueryListener {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        // Note: this assumes there is only one query active in the `testStream` method.
        //注意：这里假定`testStream`方法中只有一个查询是活动的。
        Thread.currentThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
          override def uncaughtException(t: Thread, e: Throwable): Unit = {
            streamThreadDeathCause = e
          }
        })
      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {}
      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
    }
    sparkSession.streams.addListener(listener)

    // If the test doesn't manually start the stream, we do it automatically at the beginning.
    //如果测试没有手动启动流,我们会在开始时自动执行
    val startedManually =
      actions.takeWhile(!_.isInstanceOf[StreamMustBeRunning]).exists(_.isInstanceOf[StartStream])
    val startedTest = if (startedManually) actions else StartStream() +: actions

    def testActions = actions.zipWithIndex.map {
      case (a, i) =>
        if ((pos == i && startedManually) || (pos == (i + 1) && !startedManually)) {
          "=> " + a.toString
        } else {
          "   " + a.toString
        }
    }.mkString("\n")

    def currentOffsets =
      if (currentStream != null) currentStream.committedOffsets.toString else "not started"

    def threadState =
      if (currentStream != null && currentStream.microBatchThread.isAlive) "alive" else "dead"
    def threadStackTrace = if (currentStream != null && currentStream.microBatchThread.isAlive) {
      s"Thread stack trace: ${currentStream.microBatchThread.getStackTrace.mkString("\n")}"
    } else {
      ""
    }

    def testState =
      s"""
         |== Progress ==
         |$testActions
         |
         |== Stream ==
         |Output Mode: $outputMode
         |Stream state: $currentOffsets
         |Thread state: $threadState
         |$threadStackTrace
         |${if (streamThreadDeathCause != null) stackTraceToString(streamThreadDeathCause) else ""}
         |
         |== Sink ==
         |${sink.toDebugString}
         |
         |
         |== Plan ==
         |${if (currentStream != null) currentStream.lastExecution else ""}
         """.stripMargin
    //校验
    def verify(condition: => Boolean, message: String): Unit = {
      if (!condition) {
        failTest(message)
      }
    }
    //终于
    def eventually[T](message: String)(func: => T): T = {
      try {
        Eventually.eventually(Timeout(streamingTimeout)) {
          func
        }
      } catch {
        case NonFatal(e) =>
          failTest(message, e)
      }
    }
    //失败测试
    def failTest(message: String, cause: Throwable = null) = {

      // Recursively pretty print a exception with truncated stacktrace and internal cause
      //用递归的堆栈跟踪和内部原因递归地打印一个异常
      def exceptionToString(e: Throwable, prefix: String = ""): String = {
        val base = s"$prefix${e.getMessage}" +
          e.getStackTrace.take(10).mkString(s"\n$prefix", s"\n$prefix\t", "\n")
        if (e.getCause != null) {
          base + s"\n$prefix\tCaused by: " + exceptionToString(e.getCause, s"$prefix\t")
        } else {
          base
        }
      }
      val c = Option(cause).map(exceptionToString(_))
      val m = if (message != null && message.size > 0) Some(message) else None
      fail(
        s"""
           |${(m ++ c).mkString(": ")}
           |$testState
         """.stripMargin)
    }

    val metadataRoot = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    println("====="+metadataRoot)
    var manualClockExpectedTime = -1L
    try {
      startedTest.foreach { action =>
        //处理测试流操作
        logInfo(s"Processing test stream action: $action")
        action match {
          case StartStream(trigger, triggerClock, additionalConfs) =>
            verify(currentStream == null, "stream already running")
            verify(triggerClock.isInstanceOf[SystemClock]
              || triggerClock.isInstanceOf[StreamManualClock],
              "Use either SystemClock or StreamManualClock to start the stream")
            if (triggerClock.isInstanceOf[StreamManualClock]) {
              manualClockExpectedTime = triggerClock.asInstanceOf[StreamManualClock].getTimeMillis()
            }

            additionalConfs.foreach(pair => {
              val value =
                if (sparkSession.conf.contains(pair._1)) {
                  Some(sparkSession.conf.get(pair._1))
                } else None
              resetConfValues(pair._1) = value
              sparkSession.conf.set(pair._1, pair._2)
            })

            lastStream = currentStream
            currentStream =
              sparkSession
                .streams
                .startQuery(
                  None,
                  Some(metadataRoot),
                  stream,
                  sink,
                  outputMode,
                  trigger = trigger,
                  triggerClock = triggerClock)
                .asInstanceOf[StreamingQueryWrapper]
                .streamingQuery
            // Wait until the initialization finishes, because some tests need to use `logicalPlan`
            // after starting the query.
            //等待初始化完成，因为在开始查询之后，有些测试需要使用`logicalPlan`
            try {
              currentStream.awaitInitialization(streamingTimeout.toMillis)
            } catch {
              case _: StreamingQueryException =>
                // Ignore the exception. `StopStream` or `ExpectFailure` will catch it as well.
                // 忽略异常,`StopStream`或者`ExpectFailure`也会抓住它
            }

          case AdvanceManualClock(timeToAdd) =>
            //当一个流不运行时,不能提前手动时钟
            verify(currentStream != null,
                   "can not advance manual clock when a stream is not running")
            verify(currentStream.triggerClock.isInstanceOf[StreamManualClock],
                   s"can not advance clock of type ${currentStream.triggerClock.getClass}")
            val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
            assert(manualClockExpectedTime >= 0)

            // Make sure we don't advance ManualClock too early. See SPARK-16002.
            //StreamManualClock尚未进入等待状态
            eventually("StreamManualClock has not yet entered the waiting state") {
              assert(clock.isStreamWaitingAt(manualClockExpectedTime))
            }

            clock.advance(timeToAdd)
            manualClockExpectedTime += timeToAdd
            verify(clock.getTimeMillis() === manualClockExpectedTime,
              //意外的时间更新后的时间
              s"Unexpected clock time after updating: " +
                s"expecting $manualClockExpectedTime, current ${clock.getTimeMillis()}")

          case StopStream => //无法停止未运行的流
            verify(currentStream != null, "can not stop a stream that is not running")
            try failAfter(streamingTimeout) {
              currentStream.stop()
              verify(!currentStream.microBatchThread.isAlive,
                s"microbatch thread not stopped")
              verify(!currentStream.isActive,
                "query.isActive() is false even after stopping")
              verify(currentStream.exception.isEmpty,
                s"query.exception() is not empty after clean stop: " +
                  currentStream.exception.map(_.toString()).getOrElse(""))
            } catch {
              case _: InterruptedException =>
              case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                //超时停止并等待microbatchthread终止
                failTest(
                  "Timed out while stopping and waiting for microbatchthread to terminate.", e)
              case t: Throwable =>
                failTest("Error while stopping stream", t)
            } finally {
              lastStream = currentStream
              currentStream = null
            }

          case ef: ExpectFailure[_] =>
            //当流不运行时不能期望失败
            verify(currentStream != null, "can not expect failure when stream is not running")
            try failAfter(streamingTimeout) {
              val thrownException = intercept[StreamingQueryException] {
                currentStream.awaitTermination()
              }
              //在终止失败之后,microbatch线程不会停止
              eventually("microbatch thread not stopped after termination with failure") {
                assert(!currentStream.microBatchThread.isAlive)
              }
              verify(currentStream.exception === Some(thrownException),
                s"incorrect exception returned by query.exception()")

              val exception = currentStream.exception.get
              verify(exception.cause.getClass === ef.causeClass,
                "incorrect cause in exception returned by query.exception()\n" +
                  s"\tExpected: ${ef.causeClass}\n\tReturned: ${exception.cause.getClass}")
              if (ef.isFatalError) {
                // This is a fatal error, `streamThreadDeathCause` should be set to this error in
                // UncaughtExceptionHandler.
                verify(streamThreadDeathCause != null &&
                  streamThreadDeathCause.getClass === ef.causeClass,
                  "UncaughtExceptionHandler didn't receive the correct error\n" +
                    s"\tExpected: ${ef.causeClass}\n\tReturned: $streamThreadDeathCause")
                streamThreadDeathCause = null
              }
              ef.assertFailure(exception.getCause)
            } catch {
              case _: InterruptedException =>
              case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
                //在等待失败时超时
                failTest("Timed out while waiting for failure", e)
              case t: Throwable =>  //检查流失败时出错
                failTest("Error while checking stream failure", t)
            } finally {
              lastStream = currentStream
              currentStream = null
            }

          case a: AssertOnQuery =>
            verify(currentStream != null || lastStream != null,
              //当没有流启动时不能断言
              "cannot assert when no stream has been started")
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            //verify(a.condition(streamToAssert), s"Assert on query failed: ${a.message}")

          case a: Assert =>
            val streamToAssert = Option(currentStream).getOrElse(lastStream)
            verify({ a.run(); true }, s"Assert failed: ${a.message}")

          case a: AddData =>
            try {

              // If the query is running with manual clock, then wait for the stream execution
              // thread to start waiting for the clock to increment. This is needed so that we
              // are adding data when there is no trigger that is active. This would ensure that
              // the data gets deterministically added to the next batch triggered after the manual
              // clock is incremented in following AdvanceManualClock. This avoid race conditions
              // between the test thread and the stream execution thread in tests using manual
              // clock.
              //如果查询以手动时钟运行,则等待流执行线程开始等待时钟递增,这是需要的,
              //以便我们在没有激活触发器的情况下添加数据,
              // 这将确保数据被确定性地添加到在手动时钟在AdvanceManualClock之后递增之后触发的下一批,
              // 这可以避免使用手动时钟的测试中测试线程与流执行线程之间的竞争状况
              if (currentStream != null &&
                  currentStream.triggerClock.isInstanceOf[StreamManualClock]) {
                val clock = currentStream.triggerClock.asInstanceOf[StreamManualClock]
                eventually("Error while synchronizing with manual clock before adding data") {
                  if (currentStream.isActive) {
                    assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
                  }
                }
                if (!currentStream.isActive) {
                  failTest("Query terminated while synchronizing with manual clock")
                }
              }
              // Add data
              val queryToUse = Option(currentStream).orElse(Option(lastStream))
              val (source, offset) = a.addData(queryToUse)

              def findSourceIndex(plan: LogicalPlan): Option[Int] = {
                plan
                  .collect { case StreamingExecutionRelation(s, _) => s }
                  .zipWithIndex
                  .find(_._1 == source)
                  .map(_._2)
              }

              // Try to find the index of the source to which data was added. Either get the index
              // from the current active query or the original input logical plan.
              //尝试找到添加数据的源的索引,从当前活动查询或原始输入逻辑计划中获取索引
              val sourceIndex =
                queryToUse.flatMap { query =>
                  findSourceIndex(query.logicalPlan)
                }.orElse {
                  findSourceIndex(stream.logicalPlan)
                }.getOrElse {
                  throw new IllegalArgumentException(
                    "Could find index of the source to which data was added")
                }

              // Store the expected offset of added data to wait for it later
              //存储所添加数据的预期偏移量,稍后等待
              awaiting.put(sourceIndex, offset)
            } catch {
              case NonFatal(e) =>
                failTest("Error adding data", e)
            }

          case e: ExternalAction =>
            e.runAction()

          case CheckAnswerRows(expectedAnswer, lastOnly, isSorted) =>
            verify(currentStream != null, "stream not running")
            // Get the map of source index to the current source objects
            //获取源索引的map到当前源对象
            val indexToSource = currentStream
              .logicalPlan
              .collect { case StreamingExecutionRelation(s, _) => s }
              .zipWithIndex
              .map(_.swap)
              .toMap

            // Block until all data added has been processed for all the source
            //阻塞直到所有数据源都被处理完为止
            awaiting.foreach { case (sourceIndex, offset) =>
              failAfter(streamingTimeout) {
                currentStream.awaitOffset(indexToSource(sourceIndex), offset)
              }
            }

            val sparkAnswer = try if (lastOnly) sink.latestBatchData else sink.allData catch {
              case e: Exception => //从接收器获取数据时出现异常
                failTest("Exception while getting data from sink", e)
            }

            QueryTest.sameRows(expectedAnswer, sparkAnswer, isSorted).foreach {
              error => failTest(error)
            }
        }
        pos += 1
      }
      if (streamThreadDeathCause != null) {
        failTest("Stream Thread Died", streamThreadDeathCause)
      }
    } catch {
      case _: InterruptedException if streamThreadDeathCause != null =>
        failTest("Stream Thread Died", streamThreadDeathCause)
      case e: org.scalatest.exceptions.TestFailedDueToTimeoutException =>
        failTest("Timed out waiting for stream", e)
    } finally {
      if (currentStream != null && currentStream.microBatchThread.isAlive) {
        currentStream.stop()
      }

      // Rollback prev configuration values
      //回滚prev配置值
      resetConfValues.foreach {
        case (key, Some(value)) => sparkSession.conf.set(key, value)
        case (key, None) => sparkSession.conf.unset(key)
      }
      sparkSession.streams.removeListener(listener)
    }
  }


  /**
   * Creates a stress test that randomly starts/stops/adds data/checks the result.
   * 创建一个随机启动/停止/添加数据/检查结果的压力测试
   * @param ds a dataframe that executes + 1 on a stream of integers, returning the result
    *           一个dataframe,执行+ 1流的整数,返回结果
   * @param addData an add data action that adds the given numbers to the stream, encoding them
   *                as needed
    *               添加数据操作,将给定的数字添加到流中,根据需要对它们进行编码
   * @param iterations the iteration number 迭代次数
   */
  def runStressTest(
    ds: Dataset[Int],
    addData: Seq[Int] => StreamAction,
    iterations: Int = 100): Unit = {
    runStressTest(ds, Seq.empty, (data, running) => addData(data), iterations)
  }

  /**
   * Creates a stress test that randomly starts/stops/adds data/checks the result.
   * 创建一个随机启动/停止/添加数据/检查结果的压力测试
   * @param ds a dataframe that executes + 1 on a stream of integers, returning the result
    *           一个dataframe,执行+ 1流的整数,返回结果
   * @param prepareActions actions need to run before starting the stress test.
    *                       启动压力测试之前需要运行操作
   * @param addData an add data action that adds the given numbers to the stream, encoding them
   *                as needed
    *                添加数据操作,将给定的数字添加到流中,根据需要对它们进行编码
   * @param iterations the iteration number 迭代次数
   */
  def runStressTest(
      ds: Dataset[Int],
      prepareActions: Seq[StreamAction],
      addData: (Seq[Int], Boolean) => StreamAction,
      iterations: Int): Unit = {
    implicit val intEncoder = ExpressionEncoder[Int]()
    var dataPos = 0
    var running = true
    val actions = new ArrayBuffer[StreamAction]()
    actions ++= prepareActions

    def addCheck() = { actions += CheckAnswer(1 to dataPos: _*) }

    def addRandomData() = {
      val numItems = Random.nextInt(10)
      val data = dataPos until (dataPos + numItems)
      dataPos += numItems
      actions += addData(data, running)
    }

    (1 to iterations).foreach { i =>
      val rand = Random.nextDouble()
      if(!running) {
        rand match {
          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StartStream
            actions += StartStream()
            running = true
        }
      } else {
        rand match {
          case r if r < 0.1 =>
            addCheck()

          case r if r < 0.7 => // AddData
            addRandomData()

          case _ => // StopStream
            addCheck()
            actions += StopStream
            running = false
        }
      }
    }
    if(!running) { actions += StartStream() }
    addCheck()
    testStream(ds)(actions: _*)
  }
  //等待终止测试
  object AwaitTerminationTester {

    trait ExpectedBehavior

    /** Expect awaitTermination to not be blocked
      * 期待等待终止不被阻止*/
    case object ExpectNotBlocked extends ExpectedBehavior

    /** Expect awaitTermination to get blocked
      * 期待等待终止被阻止*/
    case object ExpectBlocked extends ExpectedBehavior

    /** Expect awaitTermination to throw an exception
      * 期待等待终止抛出异常*/
    case class ExpectException[E <: Exception]()(implicit val t: ClassTag[E])
      extends ExpectedBehavior

    private val DEFAULT_TEST_TIMEOUT = 1.second

    def test(
        expectedBehavior: ExpectedBehavior,
        awaitTermFunc: () => Unit,
        testTimeout: Span = DEFAULT_TEST_TIMEOUT
      ): Unit = {

      expectedBehavior match {
        case ExpectNotBlocked => //当预期的非阻塞时被阻塞
          withClue("Got blocked when expected non-blocking.") {
            failAfter(testTimeout) {
              awaitTermFunc()
            }
          }

        case ExpectBlocked => //预期时没有被阻塞
          withClue("Was not blocked when expected.") {
            intercept[TestFailedDueToTimeoutException] {
              failAfter(testTimeout) {
                awaitTermFunc()
              }
            }
          }

        case e: ExpectException[_] =>
          val thrownException =
            withClue(s"Did not throw ${e.t.runtimeClass.getSimpleName} when expected.") {
              intercept[StreamingQueryException] {
                failAfter(testTimeout) {
                  awaitTermFunc()
                }
              }
            }
          assert(thrownException.cause.getClass === e.t.runtimeClass,
            //抛出不正确的类型的异常
            "exception of incorrect type was throw")
      }
    }
  }
}
