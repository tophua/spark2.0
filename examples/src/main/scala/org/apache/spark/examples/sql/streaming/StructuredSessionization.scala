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

// scalastyle:off println
package org.apache.spark.examples.sql.streaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._


/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 * 统计UTF8编码的文字，从网络收到的'\ n'分隔文本。
 * Usage: MapGroupsWithState <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example sql.streaming.StructuredSessionization
 * localhost 9999`
 */
object StructuredSessionization {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredSessionization")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    //创建DataFrame，表示从连接到主机：端口的输入行的流
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, treat words as sessionId of events
    //将行分成单词,将单词视为事件的sessionId
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) =>
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    //会议化事件,跟踪事件数,会话的开始和结束时间戳以及报告会话更新。
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

          // If timed out, then remove session and send final update
          //如果超时,则删除会话并发送最终更新
          if (state.hasTimedOut) {
            val finalUpdate =
              SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
            state.remove()
            finalUpdate
          } else {
            // Update start and end timestamps in session
            //更新会话中的开始和结束时间戳
            val timestamps = events.map(_.timestamp.getTime).toSeq
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              SessionInfo(
                oldSession.numEvents + timestamps.size,
                oldSession.startTimestampMs,
                math.max(oldSession.endTimestampMs, timestamps.max))
            } else {
              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)

            // Set timeout such that the session will be expired if no data received for 10 seconds
            //设置超时,如果没有数据收到10秒钟,会话将过期
            state.setTimeoutDuration("10 seconds")
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
          }
      }

    // Start running the query that prints the session updates to the console
    //开始运行将会话更新打印到控制台的查询
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
/**
  * User-defined data type representing the input events
  * 表示输入事件的用户定义的数据类型
  * */
case class Event(sessionId: String, timestamp: Timestamp)

/**
 * User-defined data type for storing a session information as state in mapGroupsWithState.
  * 用户定义的数据类型,用于将会话信息作为状态存储在mapGroupsWithState中
 *
 * @param numEvents        total number of events received in the session 在会议中收到的事件总数
 * @param startTimestampMs timestamp of first event received in the session when it started
  *                        开始时会话中收到的第一个事件的时间戳
 * @param endTimestampMs   timestamp of last event received in the session before it expired
  *                         在会话过期之前收到的最后一个事件的时间戳
 */
case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long) {

  /**
    * Duration of the session, between the first and last events
    * 会话的持续时间,在第一个和最后一个事件之间
    * */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
 * User-defined data type representing the update information returned by mapGroupsWithState.
  * 表示mapGroupsWithState返回的更新信息的用户定义的数据类型
 *
 * @param id          Id of the session 会话的ID
 * @param durationMs  Duration the session was active, that is, from first event to its expiry
  *                    会议活动持续时间,即从第一个事件到其到期时间
 * @param numEvents   Number of events received by the session while it was active
  *                    活动时会话接收到的事件数
 * @param expired     Is the session active or expired 会话是活动还是过期
 */
case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean)

// scalastyle:on println

