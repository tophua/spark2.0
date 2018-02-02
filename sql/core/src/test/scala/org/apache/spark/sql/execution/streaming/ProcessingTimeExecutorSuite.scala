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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.eclipse.jetty.util.ConcurrentHashSet
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.TimeLimits._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.util.StreamManualClock
//处理时间执行器套件
class ProcessingTimeExecutorSuite extends SparkFunSuite {

  val timeout = 10.seconds
  //下一批次
  test("nextBatchTime") {
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(100))
    assert(processingTimeExecutor.nextBatchTime(0) === 100)
    assert(processingTimeExecutor.nextBatchTime(1) === 100)
    assert(processingTimeExecutor.nextBatchTime(99) === 100)
    assert(processingTimeExecutor.nextBatchTime(100) === 200)
    assert(processingTimeExecutor.nextBatchTime(101) === 200)
    assert(processingTimeExecutor.nextBatchTime(150) === 200)
  }
  //触发时间
  test("trigger timing") {
    val triggerTimes = new ConcurrentHashSet[Int]
    val clock = new StreamManualClock()
    @volatile var continueExecuting = true
    @volatile var clockIncrementInTrigger = 0L
    val executor = ProcessingTimeExecutor(ProcessingTime("1000 milliseconds"), clock)
    val executorThread = new Thread() {
      override def run(): Unit = {
        executor.execute(() => {
          // Record the trigger time, increment clock if needed and
          //记录触发时间，如果需要，增加时钟
          triggerTimes.add(clock.getTimeMillis.toInt)
          clock.advance(clockIncrementInTrigger)
          clockIncrementInTrigger = 0 // reset this so that there are no runaway triggers
          continueExecuting
        })
      }
    }
    executorThread.start()
    // First batch should execute immediately, then executor should wait for next one
    //第一批应立即执行，然后执行者应等待下一批
    eventually {
      assert(triggerTimes.contains(0))
      assert(clock.isStreamWaitingAt(0))
      assert(clock.isStreamWaitingFor(1000))
    }

    // Second batch should execute when clock reaches the next trigger time.
    //当时钟达到下一个触发时间时，应执行第二批。
    // If next trigger takes less than the trigger interval, executor should wait for next one
    //如果下一个触发器的触发时间小于触发时间间隔，执行程序应等待下一个触发时间
    clockIncrementInTrigger = 500
    clock.setTime(1000)
    eventually {
      assert(triggerTimes.contains(1000))
      assert(clock.isStreamWaitingAt(1500))
      assert(clock.isStreamWaitingFor(2000))
    }

    // If next trigger takes less than the trigger interval, executor should immediately execute
    // another one
    //如果下一个触发器的触发时间小于触发时间间隔，执行程序应立即执行另一个触发器
    clockIncrementInTrigger = 1500
    //通过设置时钟到2000允许另一个触发器
    clock.setTime(2000)   // allow another trigger by setting clock to 2000
    eventually {
      // Since the next trigger will take 1500 (which is more than trigger interval of 1000)
      // executor will immediately execute another trigger
      //由于下一个触发器将花费1500（大于触发器间隔1000）,执行器将立即执行另一个触发器
      assert(triggerTimes.contains(2000) && triggerTimes.contains(3500))
      assert(clock.isStreamWaitingAt(3500))
      assert(clock.isStreamWaitingFor(4000))
    }
    continueExecuting = false
    clock.advance(1000)
    waitForThreadJoin(executorThread)
  }
  //调用nextBatchTime与前一个调用的结果应返回下一个时间间隔
  test("calling nextBatchTime with the result of a previous call should return the next interval") {
    val intervalMS = 100
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(intervalMS))

    val ITERATION = 10
    var nextBatchTime: Long = 0
    for (it <- 1 to ITERATION) {
      nextBatchTime = processingTimeExecutor.nextBatchTime(nextBatchTime)
    }

    // nextBatchTime should be 1000
    //下一个批次应该是1000
    assert(nextBatchTime === intervalMS * ITERATION)
  }

  private def testBatchTermination(intervalMs: Long): Unit = {
    var batchCounts = 0
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(intervalMs))
    processingTimeExecutor.execute(() => {
      batchCounts += 1
      // If the batch termination works correctly, batchCounts should be 3 after `execute`
      //如果批处理终止正确，batchCounts在执行后应该是3
      batchCounts < 3
    })
    assert(batchCounts === 3)
  }
  //批量终止
  test("batch termination") {
    testBatchTermination(0)
    testBatchTermination(10)
  }
  //通知批量落后
  test("notifyBatchFallingBehind") {
    val clock = new StreamManualClock()
    @volatile var batchFallingBehindCalled = false
    val t = new Thread() {
      override def run(): Unit = {
        val processingTimeExecutor = new ProcessingTimeExecutor(ProcessingTime(100), clock) {
          override def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
            batchFallingBehindCalled = true
          }
        }
        processingTimeExecutor.execute(() => {
          clock.waitTillTime(200)
          false
        })
      }
    }
    t.start()
    // Wait until the batch is running so that we don't call `advance` too early
    //等到批量运行，以免我们太早调用“advance”
    eventually { assert(clock.isStreamWaitingFor(200)) }
    clock.advance(200)
    waitForThreadJoin(t)
    assert(batchFallingBehindCalled === true)
  }

  private def eventually(body: => Unit): Unit = {
    Eventually.eventually(Timeout(timeout)) { body }
  }

  private def waitForThreadJoin(thread: Thread): Unit = {
    failAfter(timeout) { thread.join() }
  }
}
