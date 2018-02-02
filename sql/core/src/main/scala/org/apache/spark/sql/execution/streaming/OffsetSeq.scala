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

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.internal.SQLConf.{SHUFFLE_PARTITIONS, STATE_STORE_PROVIDER_CLASS}

/**
 * An ordered collection of offsets, used to track the progress of processing data from one or more
 * [[Source]]s that are present in a streaming query. This is similar to simplified, single-instance
 * vector clock that must progress linearly forward.
  * 偏序的有序集合,用于跟踪处理来自流式查询中存在的一个或多个[[Source]]的数据的进度,
  * 这与简化的单实例向量时钟类似,必须线性前进。
 */
case class OffsetSeq(offsets: Seq[Option[Offset]], metadata: Option[OffsetSeqMetadata] = None) {

  /**
   * Unpacks an offset into [[StreamProgress]] by associating each offset with the order list of
   * sources.
   * 通过将每个偏移量与源的顺序列表相关联,将偏移量解包到[[StreamProgress]]中。
   * This method is typically used to associate a serialized offset with actual sources (which
   * cannot be serialized).
    * 此方法通常用于将序列化偏移量与实际源(不能序列化)相关联
   */
  def toStreamProgress(sources: Seq[Source]): StreamProgress = {
    assert(sources.size == offsets.size)
    new StreamProgress ++ sources.zip(offsets).collect { case (s, Some(o)) => (s, o) }
  }

  override def toString: String =
    offsets.map(_.map(_.json).getOrElse("-")).mkString("[", ", ", "]")
}

object OffsetSeq {

  /**
   * Returns a [[OffsetSeq]] with a variable sequence of offsets.
    * 以可变的偏移序列返回[[OffsetSeq]]
   * `nulls` in the sequence are converted to `None`s.
   */
  def fill(offsets: Offset*): OffsetSeq = OffsetSeq.fill(None, offsets: _*)

  /**
   * Returns a [[OffsetSeq]] with metadata and a variable sequence of offsets.
    * 用元数据和一个可变的偏移序列返回[[OffsetSeq]]
   * `nulls` in the sequence are converted to `None`s.
   */
  def fill(metadata: Option[String], offsets: Offset*): OffsetSeq = {
    OffsetSeq(offsets.map(Option(_)), metadata.map(OffsetSeqMetadata.apply))
  }
}


/**
 * Contains metadata associated with a [[OffsetSeq]]. This information is
 * persisted to the offset log in the checkpoint location via the [[OffsetSeq]] metadata field.
 * 包含与[[OffsetSeq]]关联的元数据,该信息通过[[OffsetSeq]]元数据字段持久保存到检查点位置的偏移日志中。
 * @param batchWatermarkMs: The current eventTime watermark, used to
 * bound the lateness of data that will processed. Time unit: milliseconds
  * 当前的eventTime水印，用于限制将要处理的数据的迟后。 时间单位：毫秒
 * @param batchTimestampMs: The current batch processing timestamp. 当前批处理时间戳
 * Time unit: milliseconds 时间单位：毫秒
 * @param conf: Additional conf_s to be persisted across batches, e.g. number of shuffle partitions.
  *            另外的批次可以在批次之间保持,例如 洗牌分区的数量
 */
case class OffsetSeqMetadata(
    batchWatermarkMs: Long = 0,
    batchTimestampMs: Long = 0,
    conf: Map[String, String] = Map.empty) {
  def json: String = Serialization.write(this)(OffsetSeqMetadata.format)
}

object OffsetSeqMetadata extends Logging {
  private implicit val format = Serialization.formats(NoTypeHints)
  private val relevantSQLConfs = Seq(SHUFFLE_PARTITIONS, STATE_STORE_PROVIDER_CLASS)

  def apply(json: String): OffsetSeqMetadata = Serialization.read[OffsetSeqMetadata](json)

  def apply(
      batchWatermarkMs: Long,
      batchTimestampMs: Long,
      sessionConf: RuntimeConfig): OffsetSeqMetadata = {
    val confs = relevantSQLConfs.map { conf => conf.key -> sessionConf.get(conf.key) }.toMap
    OffsetSeqMetadata(batchWatermarkMs, batchTimestampMs, confs)
  }

  /** Set the SparkSession configuration with the values in the metadata
    * 使用元数据中的值设置SparkSession配置 */
  def setSessionConf(metadata: OffsetSeqMetadata, sessionConf: RuntimeConfig): Unit = {
    OffsetSeqMetadata.relevantSQLConfs.map(_.key).foreach { confKey =>

      metadata.conf.get(confKey) match {

        case Some(valueInMetadata) =>
          // Config value exists in the metadata, update the session config with this value
          //配置值存在于元数据中,用此值更新会话配置
          val optionalValueInSession = sessionConf.getOption(confKey)
          if (optionalValueInSession.isDefined && optionalValueInSession.get != valueInMetadata) {
            logWarning(s"Updating the value of conf '$confKey' in current session from " +
              s"'${optionalValueInSession.get}' to '$valueInMetadata'.")
          }
          sessionConf.set(confKey, valueInMetadata)

        case None =>
          // For backward compatibility, if a config was not recorded in the offset log,
          //为了向后兼容，如果配置没有被记录在偏移日志中
          // then log it, and let the existing conf value in SparkSession prevail.
          //然后记录下来，并让SparkSession中现有的conf值占上风
          logWarning (s"Conf '$confKey' was not found in the offset log, using existing value")
      }
    }
  }
}
