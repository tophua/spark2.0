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
import org.apache.spark.scheduler.SparkListenerEvent

/**
 * Interface for listening to events related to [[StreamingQuery StreamingQueries]].
  * 用于监听与[[StreamingQuery StreamingQueries]]相关的事件的接口
 * @note The methods are not thread-safe as they may be called from different threads.
  *       这些方法不是线程安全的,因为它们可以从不同的线程中调用
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
abstract class StreamingQueryListener {

  import StreamingQueryListener._

  /**
   * Called when a query is started.查询开始时调用
   * @note This is called synchronously with
   *       [[org.apache.spark.sql.streaming.DataStreamWriter `DataStreamWriter.start()`]],
   *       that is, `onQueryStart` will be called on all listeners before
   *       `DataStreamWriter.start()` returns the corresponding [[StreamingQuery]]. Please
   *       don't block this method as it will block your query.
   * @since 2.0.0
   */
  def onQueryStarted(event: QueryStartedEvent): Unit

  /**
   * Called when there is some status update (ingestion rate updated, etc.)
   * 当有一些状态更新时调用(更新摄取率等)
   * @note This method is asynchronous. The status in [[StreamingQuery]] will always be
   *       latest no matter when this method is called. Therefore, the status of [[StreamingQuery]]
   *       may be changed before/when you process the event. E.g., you may find [[StreamingQuery]]
   *       is terminated when you are processing `QueryProgressEvent`.
   * @since 2.0.0
   */
  def onQueryProgress(event: QueryProgressEvent): Unit

  /**
   * Called when a query is stopped, with or without error.
    * 当查询停止时调用,有或没有错误
   * @since 2.0.0
   */
  def onQueryTerminated(event: QueryTerminatedEvent): Unit
}


/**
 * Companion object of [[StreamingQueryListener]] that defines the listener events.
 * @since 2.0.0
 */
@InterfaceStability.Evolving
object StreamingQueryListener {

  /**
   * Base type of [[StreamingQueryListener]] events
    * [StreamingQueryListener]事件的基本类型
   * @since 2.0.0
   */
  @InterfaceStability.Evolving
  trait Event extends SparkListenerEvent

  /**
   * Event representing the start of a query
    * 表示查询开始的事件
   * @param id An unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param name User-specified name of the query, null if not specified.
   * @since 2.1.0
   */
  @InterfaceStability.Evolving
  class QueryStartedEvent private[sql](
      val id: UUID,
      val runId: UUID,
      val name: String) extends Event

  /**
   * Event representing any progress updates in a query.
    * 表示查询中任何进度更新的事件
   * @param progress The query progress updates.
   * @since 2.1.0
   */
  @InterfaceStability.Evolving
  class QueryProgressEvent private[sql](val progress: StreamingQueryProgress) extends Event

  /**
   * Event representing that termination of a query.
   * 表示查询终止的事件
   * @param id An unique query id that persists across restarts. See `StreamingQuery.id()`.
   * @param runId A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
   * @param exception The exception message of the query if the query was terminated
   *                  with an exception. Otherwise, it will be `None`.
   * @since 2.1.0
   */
  @InterfaceStability.Evolving
  class QueryTerminatedEvent private[sql](
      val id: UUID,
      val runId: UUID,
      val exception: Option[String]) extends Event
}
