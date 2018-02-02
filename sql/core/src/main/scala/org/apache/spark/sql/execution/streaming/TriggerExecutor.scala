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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.{Clock, SystemClock}

trait TriggerExecutor {

  /**
   * Execute batches using `batchRunner`. If `batchRunner` runs `false`, terminate the execution.
    * 执行批量使用` batchrunner `,如果` batchrunner `运行`false`,终止执行
   */
  def execute(batchRunner: () => Boolean): Unit
}

/**
 * A trigger executor that runs a single batch only, then terminates.
  * 触发器执行器只运行单个批处理,然后终止
 */
case class OneTimeExecutor() extends TriggerExecutor {

  /**
   * Execute a single batch using `batchRunner`.
    * 执行一个单一的批量使用` batchrunner `
   */
  override def execute(batchRunner: () => Boolean): Unit = batchRunner()
}

/**
 * A trigger executor that runs a batch every `intervalMs` milliseconds.
  * 触发执行器运行一个批每` intervalms `毫秒
 */
case class ProcessingTimeExecutor(processingTime: ProcessingTime, clock: Clock = new SystemClock())
  extends TriggerExecutor with Logging {

  private val intervalMs = processingTime.intervalMs
  require(intervalMs >= 0)

  override def execute(triggerHandler: () => Boolean): Unit = {
    while (true) {
      val triggerTimeMs = clock.getTimeMillis
      val nextTriggerTimeMs = nextBatchTime(triggerTimeMs)
      val terminated = !triggerHandler()
      if (intervalMs > 0) {
        val batchElapsedTimeMs = clock.getTimeMillis - triggerTimeMs
        if (batchElapsedTimeMs > intervalMs) {
          notifyBatchFallingBehind(batchElapsedTimeMs)
        }
        if (terminated) {
          return
        }
        clock.waitTillTime(nextTriggerTimeMs)
      } else {
        if (terminated) {
          return
        }
      }
    }
  }

  /** Called when a batch falls behind
    * 当批次失败时调用*/
  def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
    logWarning("Current batch is falling behind. The trigger interval is " +
      s"${intervalMs} milliseconds, but spent ${realElapsedTimeMs} milliseconds")
  }

  /**
   * Returns the start time in milliseconds for the next batch interval, given the current time.
    * 在给定当前时间的情况下,返回下一个批处理间隔的开始时间(以毫秒为单位)
   * Note that a batch interval is inclusive with respect to its start time, and thus calling
   * `nextBatchTime` with the result of a previous call should return the next interval. (i.e. given
   * an interval of `100 ms`, `nextBatchTime(nextBatchTime(0)) = 200` rather than `0`).
    * 请注意,批处理时间间隔是相对于其开始时间而言的,因此使用前一个调用的结果调用nextBatchTime应返回下一个时间间隔,
    * （即,给定间隔“100ms”,“nextBatchTime(nextBatchTime(0))= 200”而不是“0”
   */
  def nextBatchTime(now: Long): Long = {
    if (intervalMs == 0) now else now / intervalMs * intervalMs + intervalMs
  }
}
