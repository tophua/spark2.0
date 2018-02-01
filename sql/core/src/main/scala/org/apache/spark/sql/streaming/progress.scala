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

import java.{util => ju}
import java.lang.{Long => JLong}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.InterfaceStability

/**
 * Information about updates made to stateful operators in a [[StreamingQuery]] during a trigger.
  * 触发期间[[StreamingQuery]]中有状态操作员更新的信息
 */
@InterfaceStability.Evolving
class StateOperatorProgress private[sql](
    val numRowsTotal: Long,
    val numRowsUpdated: Long,
    val memoryUsedBytes: Long
  ) extends Serializable {

  /** The compact JSON representation of this progress.
    * 这个进展的紧凑的JSON表示 */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this progress.
    * 这个进展的漂亮的(即缩进的)JSON表示 */
  def prettyJson: String = pretty(render(jsonValue))

  private[sql] def copy(newNumRowsUpdated: Long): StateOperatorProgress =
    new StateOperatorProgress(numRowsTotal, newNumRowsUpdated, memoryUsedBytes)

  private[sql] def jsonValue: JValue = {
    ("numRowsTotal" -> JInt(numRowsTotal)) ~
    ("numRowsUpdated" -> JInt(numRowsUpdated)) ~
    ("memoryUsedBytes" -> JInt(memoryUsedBytes))
  }

  override def toString: String = prettyJson
}

/**
 * Information about progress made in the execution of a [[StreamingQuery]] during
 * a trigger. Each event relates to processing done for a single trigger of the streaming
 * query. Events are emitted even when no new data is available to be processed.
 * 在触发期间执行[[StreamingQuery]]的进度信息,每个事件都与流式查询的单个触发器的处理相关,
  * 即使没有新的数据可供处理,也会发射事件。
 * @param id An unique query id that persists across restarts. See `StreamingQuery.id()`.
 * @param runId A query id that is unique for every start/restart. See `StreamingQuery.runId()`.
 * @param name User-specified name of the query, null if not specified.
 * @param timestamp Beginning time of the trigger in ISO8601 format, i.e. UTC timestamps.
 * @param batchId A unique id for the current batch of data being processed.  Note that in the
 *                case of retries after a failure a given batchId my be executed more than once.
 *                Similarly, when there is no data to be processed, the batchId will not be
 *                incremented.
 * @param durationMs The amount of time taken to perform various operations in milliseconds.
 * @param eventTime Statistics of event time seen in this batch. It may contain the following keys:
 *                 {{{
 *                   "max" -> "2016-12-05T20:54:20.827Z"  // maximum event time seen in this trigger
 *                   "min" -> "2016-12-05T20:54:20.827Z"  // minimum event time seen in this trigger
 *                   "avg" -> "2016-12-05T20:54:20.827Z"  // average event time seen in this trigger
 *                   "watermark" -> "2016-12-05T20:54:20.827Z"  // watermark used in this trigger
 *                 }}}
 *                 All timestamps are in ISO8601 format, i.e. UTC timestamps.
 * @param stateOperators Information about operators in the query that store state.
 * @param sources detailed statistics on data being read from each of the streaming sources.
 * @since 2.1.0
 */
@InterfaceStability.Evolving
class StreamingQueryProgress private[sql](
  val id: UUID,
  val runId: UUID,
  val name: String,
  val timestamp: String,
  val batchId: Long,
  val durationMs: ju.Map[String, JLong],
  val eventTime: ju.Map[String, String],
  val stateOperators: Array[StateOperatorProgress],
  val sources: Array[SourceProgress],
  val sink: SinkProgress) extends Serializable {

  /** The aggregate (across all sources) number of records processed in a trigger.
    * 在触发器中处理的记录总数(跨越所有来源)*/
  def numInputRows: Long = sources.map(_.numInputRows).sum

  /** The aggregate (across all sources) rate of data arriving.
    * 数据到达的汇总(跨所有来源)*/
  def inputRowsPerSecond: Double = sources.map(_.inputRowsPerSecond).sum

  /** The aggregate (across all sources) rate at which Spark is processing data.
    * Spark正在处理数据的汇总(所有数据源)*/
  def processedRowsPerSecond: Double = sources.map(_.processedRowsPerSecond).sum

  /** The compact JSON representation of this progress.
    * 这个进展的紧凑的JSON表示*/
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this progress.
    * 这个进展的漂亮的(即缩进)JSON表示*/
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = prettyJson

  private[sql] def jsonValue: JValue = {
    def safeDoubleToJValue(value: Double): JValue = {
      if (value.isNaN || value.isInfinity) JNothing else JDouble(value)
    }

    /** Convert map to JValue while handling empty maps. Also, this sorts the keys.
      * 处理空地图时将地图转换为JValue,此外,这种排序的关键*/
    def safeMapToJValue[T](map: ju.Map[String, T], valueToJValue: T => JValue): JValue = {
      if (map.isEmpty) return JNothing
      val keys = map.asScala.keySet.toSeq.sorted
      keys.map { k => k -> valueToJValue(map.get(k)) : JObject }.reduce(_ ~ _)
    }

    ("id" -> JString(id.toString)) ~
    ("runId" -> JString(runId.toString)) ~
    ("name" -> JString(name)) ~
    ("timestamp" -> JString(timestamp)) ~
    ("batchId" -> JInt(batchId)) ~
    ("numInputRows" -> JInt(numInputRows)) ~
    ("inputRowsPerSecond" -> safeDoubleToJValue(inputRowsPerSecond)) ~
    ("processedRowsPerSecond" -> safeDoubleToJValue(processedRowsPerSecond)) ~
    ("durationMs" -> safeMapToJValue[JLong](durationMs, v => JInt(v.toLong))) ~
    ("eventTime" -> safeMapToJValue[String](eventTime, s => JString(s))) ~
    ("stateOperators" -> JArray(stateOperators.map(_.jsonValue).toList)) ~
    ("sources" -> JArray(sources.map(_.jsonValue).toList)) ~
    ("sink" -> sink.jsonValue)
  }
}

/**
 * Information about progress made for a source in the execution of a [[StreamingQuery]]
 * during a trigger. See [[StreamingQueryProgress]] for more information.
  * 信息取得的进展在一个streamingquery执行源时触发,看到streamingqueryprogress的更多信息。
 *
 * @param description            Description of the source. 来源说明
 * @param startOffset            The starting offset for data being read. 正在读取数据的起始偏移量
 * @param endOffset              The ending offset for data being read. 正在读取数据的结束偏移量
 * @param numInputRows           The number of records read from this source.从这个源读取的记录数
 * @param inputRowsPerSecond     The rate at which data is arriving from this source.数据从这个源到达的速率
 * @param processedRowsPerSecond The rate at which data from this source is being procressed by
 *                               速度从源数据被Spark处理
 * @since 2.1.0
 */
@InterfaceStability.Evolving
class SourceProgress protected[sql](
  val description: String,
  val startOffset: String,
  val endOffset: String,
  val numInputRows: Long,
  val inputRowsPerSecond: Double,
  val processedRowsPerSecond: Double) extends Serializable {

  /** The compact JSON representation of this progress.
    * 这一进展的紧凑JSON表示法 */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this progress.
    * 这个进程的漂亮(即缩进)JSON表示 */
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = prettyJson

  private[sql] def jsonValue: JValue = {
    def safeDoubleToJValue(value: Double): JValue = {
      if (value.isNaN || value.isInfinity) JNothing else JDouble(value)
    }

    ("description" -> JString(description)) ~
      ("startOffset" -> tryParse(startOffset)) ~
      ("endOffset" -> tryParse(endOffset)) ~
      ("numInputRows" -> JInt(numInputRows)) ~
      ("inputRowsPerSecond" -> safeDoubleToJValue(inputRowsPerSecond)) ~
      ("processedRowsPerSecond" -> safeDoubleToJValue(processedRowsPerSecond))
  }

  private def tryParse(json: String) = try {
    parse(json)
  } catch {
    case NonFatal(e) => JString(json)
  }
}

/**
 * Information about progress made for a sink in the execution of a [[StreamingQuery]]
 * during a trigger. See [[StreamingQueryProgress]] for more information.
 *
 * @param description Description of the source corresponding to this status.
 * @since 2.1.0
 */
@InterfaceStability.Evolving
class SinkProgress protected[sql](
    val description: String) extends Serializable {

  /** The compact JSON representation of this progress.
    * 这一进展的紧凑JSON表示法 */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this progress.
    * 这个进程的漂亮(即缩进)JSON表示*/
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = prettyJson

  private[sql] def jsonValue: JValue = {
    ("description" -> JString(description))
  }
}
