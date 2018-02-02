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

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamingQueryProgress, StreamingQueryStatus}

/**
 * Wrap non-serializable StreamExecution to make the query serializable as it's easy to for it to
 * get captured with normal usage. It's safe to capture the query but not use it in executors.
 * However, if the user tries to call its methods, it will throw `IllegalStateException`.
  * 将不可序列化的StreamExecution封装起来,使得查询可序列化,因为它很容易被正常使用捕获,
  * 捕获查询是安全的,但不用于执行者。
  * 但是,如果用户试图调用它的方法,它将抛出`IllegalStateException`。
 */
class StreamingQueryWrapper(@transient private val _streamingQuery: StreamExecution)
  extends StreamingQuery with Serializable {

  def streamingQuery: StreamExecution = {
    /** Assert the codes run in the driver.
      * 断言在驱动程序中运行的代码 */
    if (_streamingQuery == null) {
      throw new IllegalStateException("StreamingQuery cannot be used in executors")
    }
    _streamingQuery
  }

  override def name: String = {
    streamingQuery.name
  }

  override def id: UUID = {
    streamingQuery.id
  }

  override def runId: UUID = {
    streamingQuery.runId
  }

  override def awaitTermination(): Unit = {
    streamingQuery.awaitTermination()
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    streamingQuery.awaitTermination(timeoutMs)
  }

  override def stop(): Unit = {
    streamingQuery.stop()
  }

  override def processAllAvailable(): Unit = {
    streamingQuery.processAllAvailable()
  }

  override def isActive: Boolean = {
    streamingQuery.isActive
  }

  override def lastProgress: StreamingQueryProgress = {
    streamingQuery.lastProgress
  }

  override def explain(): Unit = {
    streamingQuery.explain()
  }

  override def explain(extended: Boolean): Unit = {
    streamingQuery.explain(extended)
  }

  /**
   * This method is called in Python. Python cannot call "explain" directly as it outputs in the JVM
   * process, which may not be visible in Python process.
    * 这个方法在Python中被调用,Python不能直接调用“explain”,因为它在JVM进程中输出,这在Python进程中可能不可见
   */
  def explainInternal(extended: Boolean): String = {
    streamingQuery.explainInternal(extended)
  }

  override def sparkSession: SparkSession = {
    streamingQuery.sparkSession
  }

  override def recentProgress: Array[StreamingQueryProgress] = {
    streamingQuery.recentProgress
  }

  override def status: StreamingQueryStatus = {
    streamingQuery.status
  }

  override def exception: Option[StreamingQueryException] = {
    streamingQuery.exception
  }
}
