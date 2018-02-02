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

import scala.collection.mutable

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.ListenerBus

/**
 * A bus to forward events to [[StreamingQueryListener]]s. This one will send received
 * [[StreamingQueryListener.Event]]s to the Spark listener bus. It also registers itself with
 * Spark listener bus, so that it can receive [[StreamingQueryListener.Event]]s and dispatch them
 * to StreamingQueryListeners.
 *
  * 一条将事件转发到[[StreamingQueryListener]]的总线,这个将发送收到的[[StreamingQueryListener.Event]]到Spark监听器总线,
  * 它还向Spark监听器总线注册自己,以便它可以接收[[StreamingQueryListener.Event]]并将它们分派给StreamingQueryListeners
  *
 * Note that each bus and its registered listeners are associated with a single SparkSession
 * and StreamingQueryManager. So this bus will dispatch events to registered listeners for only
 * those queries that were started in the associated SparkSession.
  * 请注意，每个总线及其注册的监听器都与一个SparkSession和StreamingQueryManager相关联,
  * 所以这个总线只会把注册的监听器的事件发送给在关联的SparkSession中启动的查询
 */
class StreamingQueryListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[StreamingQueryListener, StreamingQueryListener.Event] {

  import StreamingQueryListener._

//  sparkListenerBus.addListener(this)

  /**
   * RunIds of active queries whose events are supposed to be forwarded by this ListenerBus
   * to registered `StreamingQueryListeners`.
   * RunIds的活动查询的事件应该由这个ListenerBus转发到注册`StreamingQueryListeners`
    *
   * Note 1: We need to track runIds instead of ids because the runId is unique for every started
   * query, even it its a restart. So even if a query is restarted, this bus will identify them
   * separately and correctly account for the restart.
    * 注1：我们需要跟踪runId而不是id,因为runId对于每个已启动的查询都是唯一的,即使它是重新启动,
    * 所以,即使重新启动查询,该总线也会单独识别并正确考虑重新启动。
   *
   * Note 2: This list needs to be maintained separately from the
   * `StreamingQueryManager.activeQueries` because a terminated query is cleared from
   * `StreamingQueryManager.activeQueries` as soon as it is stopped, but the this ListenerBus
   * must clear a query only after the termination event of that query has been posted.
    * 注2：这个列表需要与`StreamingQueryManager.activeQueries`分开维护,
    * 因为一旦停止,就终止的查询从`StreamingQueryManager.activeQueries`中清除,
    * 但是这个ListenerBus只有在终止事件之后才能清除查询该查询已发布
   */
  private val activeQueryRunIds = new mutable.HashSet[UUID]

  /**
   * Post a StreamingQueryListener event to the added StreamingQueryListeners.
    * 将StreamingQueryListener事件发布到添加的StreamingQueryListeners
   * Note that only the QueryStarted event is posted to the listener synchronously. Other events
   * are dispatched to Spark listener bus. This method is guaranteed to be called by queries in
   * the same SparkSession as this listener.
    * 请注意,只有QueryStarted事件同步发送给侦听器,其他事件被派往Spark 监听bus,
    * 这个方法被保证在与这个监听器相同的SparkSession中被查询调用。
   */
  def post(event: StreamingQueryListener.Event) {
    event match {
      case s: QueryStartedEvent =>
        activeQueryRunIds.synchronized { activeQueryRunIds += s.runId }
        sparkListenerBus.post(s)
        // post to local listeners to trigger callbacks
        //提交到本地监听器来触发回调
        postToAll(s)
      case _ =>
        sparkListenerBus.post(event)
    }
  }

  /**
   * Override the parent `postToAll` to remove the query id from `activeQueryRunIds` after all
   * the listeners process `QueryTerminatedEvent`. (SPARK-19594)
    * 在所有侦听器处理QueryTerminatedEvent后,重写父`postToAll`以从`activeQueryRunIds`中删除查询ID
   */
  override def postToAll(event: Event): Unit = {
    super.postToAll(event)
    event match {
      case t: QueryTerminatedEvent =>
        activeQueryRunIds.synchronized { activeQueryRunIds -= t.runId }
      case _ =>
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: StreamingQueryListener.Event =>
        // SPARK-18144: we broadcast QueryStartedEvent to all listeners attached to this bus
        // synchronously and the ones attached to LiveListenerBus asynchronously. Therefore,
        // we need to ignore QueryStartedEvent if this method is called within SparkListenerBus
        // thread
        if (!LiveListenerBus.withinListenerThread.value || !e.isInstanceOf[QueryStartedEvent]) {
          postToAll(e)
        }
      case _ =>
    }
  }

  /**
   * Dispatch events to registered StreamingQueryListeners. Only the events associated queries
   * started in the same SparkSession as this ListenerBus will be dispatched to the listeners.
    * 派发事件到注册的StreamingQueryListeners,
    * 只有与此ListenerBus相同的SparkSession中启动的事件关联查询才会分派给侦听器
   */
  override protected def doPostEvent(
      listener: StreamingQueryListener,
      event: StreamingQueryListener.Event): Unit = {
    def shouldReport(runId: UUID): Boolean = {
      activeQueryRunIds.synchronized { activeQueryRunIds.contains(runId) }
    }

    event match {
      case queryStarted: QueryStartedEvent =>
        if (shouldReport(queryStarted.runId)) {
          listener.onQueryStarted(queryStarted)
        }
      case queryProgress: QueryProgressEvent =>
        if (shouldReport(queryProgress.progress.runId)) {
          listener.onQueryProgress(queryProgress)
        }
      case queryTerminated: QueryTerminatedEvent =>
        if (shouldReport(queryTerminated.runId)) {
          listener.onQueryTerminated(queryTerminated)
        }
      case _ =>
    }
  }
}
