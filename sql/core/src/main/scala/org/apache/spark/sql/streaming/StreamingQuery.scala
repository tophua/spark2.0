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

import java.util.UUID

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.SparkSession

/**
 * A handle to a query that is executing continuously in the background as new data arrives.
 * All these methods are thread-safe.
  * 在新数据到达时在后台连续执行的查询的句柄,所有这些方法都是线程安全的
 * @since 2.0.0
 */
@InterfaceStability.Evolving
trait StreamingQuery {

  /**
   * Returns the user-specified name of the query, or null if not specified.
    * 返回查询的用户指定名称,如果未指定,则返回null
   * This name can be specified in the `org.apache.spark.sql.streaming.DataStreamWriter`
   * as `dataframe.writeStream.queryName("query").start()`.
    * 这个名称可以在`org.apache.spark.sql.streaming.DataStreamWriter`中指定为 dataframe.writeStream.queryName（“query”）
   * This name, if set, must be unique across all active queries.
   * 该名称(如果已设置)在所有活动查询中必须唯一
   * @since 2.0.0
   */
  def name: String

  /**
   * Returns the unique id of this query that persists across restarts from checkpoint data.
    * 返回检查点数据重新启动时持续存在的此查询的唯一标识
   * That is, this id is generated when a query is started for the first time, and
   * will be the same every time it is restarted from checkpoint data. Also see [[runId]].
   * 也就是说,这个ID是在第一次启动查询时生成的,每次从检查点数据重新启动时都会一样,另见[[runId]]
   * @since 2.1.0
   */
  def id: UUID

  /**
   * Returns the unique id of this run of the query. That is, every start/restart of a query will
   * generated a unique runId. Therefore, every time a query is restarted from
   * checkpoint, it will have the same [[id]] but different [[runId]]s.
   */
  def runId: UUID

  /**
   * Returns the `SparkSession` associated with `this`.
   * 返回与`this`关联的`SparkSession`
   * @since 2.0.0
   */
  def sparkSession: SparkSession

  /**
   * Returns `true` if this query is actively running.
   * 如果此查询正在运行,则返回“true”
   * @since 2.0.0
   */
  def isActive: Boolean

  /**
   * Returns the [[StreamingQueryException]] if the query was terminated by an exception.
   * @since 2.0.0
   */
  def exception: Option[StreamingQueryException]

  /**
   * Returns the current status of the query.
   * 返回查询的当前状态
   * @since 2.0.2
   */
  def status: StreamingQueryStatus

  /**
   * Returns an array of the most recent [[StreamingQueryProgress]] updates for this query.
    * 返回一个数组的最新[streamingqueryprogress]更新查询
   * The number of progress updates retained for each stream is configured by Spark session
   * configuration `spark.sql.streaming.numRecentProgressUpdates`.
   * 进度更新保留每个流的数量是由 Spark的会话配置的配置,SQL numrecentprogressupdates流
   * @since 2.1.0
   */
  def recentProgress: Array[StreamingQueryProgress]

  /**
   * Returns the most recent [[StreamingQueryProgress]] update of this streaming query.
   * 返回最近的[streamingqueryprogress]更新此流查询
   * @since 2.1.0
   */
  def lastProgress: StreamingQueryProgress

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception.
    * 等待`这`查询终端，通过`查询。stop() `或异常
   * If the query has terminated with an exception, then the exception will be thrown.
   * 如果查询以一个异常终止,则抛出异常
    *
   * If the query has terminated, then all subsequent calls to this method will either return
   * immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   * 如果查询已经终止,那么所有的后续调用此方法将立即返回(如果查询是由` stop() `终止),或引发异常,立即（如果查询已终止与例外）
   * @throws StreamingQueryException if the query has terminated with an exception.
   *
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitTermination(): Unit

  /**
   * Waits for the termination of `this` query, either by `query.stop()` or by an exception.
   * If the query has terminated with an exception, then the exception will be thrown.
   * Otherwise, it returns whether the query has terminated or not within the `timeoutMs`
   * milliseconds.
   *
   * If the query has terminated, then all subsequent calls to this method will either return
   * `true` immediately (if the query was terminated by `stop()`), or throw the exception
   * immediately (if the query has terminated with exception).
   *
   * @throws StreamingQueryException if the query has terminated with an exception
   *
   * @since 2.0.0
   */
  @throws[StreamingQueryException]
  def awaitTermination(timeoutMs: Long): Boolean

  /**
   * Blocks until all available data in the source has been processed and committed to the sink.
    * 阻塞,直到源中的所有可用数据都被处理并提交到接收器
   * This method is intended for testing. Note that in the case of continually arriving data,
    * 此方法用于测试,请注意,在连续到达数据的情况下,this
   * method may block forever. Additionally, this method is only guaranteed to block until data that
   * has been synchronously appended data to a `org.apache.spark.sql.execution.streaming.Source`
   * prior to invocation. (i.e. `getOffset` must immediately reflect the addition).
    * 这个方法可能永远阻塞,此外,只有在调用之前,这种方法才能保证阻塞,
    * 直到数据被同步地附加到`org.apache.spark.sql.execution.streaming.Source`数据,(即`getOffset`必须立即反映这个加法)
   * @since 2.0.0
   */
  def processAllAvailable(): Unit

  /**
   * Stops the execution of this query if it is running. This method blocks until the threads
   * performing execution has stopped.
    * 停止执行此查询,如果它正在运行,这个方法阻塞,直到执行的线程停止。
   * @since 2.0.0
   */
  def stop(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
    * 打印物理计划到控制台进行调试
   * @since 2.0.0
   */
  def explain(): Unit

  /**
   * Prints the physical plan to the console for debugging purposes.
   * 打印物理计划到控制台进行调试
   * @param extended whether to do extended explain or not
   * @since 2.0.0
   */
  def explain(extended: Boolean): Unit
}
