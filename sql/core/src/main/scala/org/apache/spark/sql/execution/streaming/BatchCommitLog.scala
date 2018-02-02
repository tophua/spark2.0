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

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets._

import scala.io.{Source => IOSource}

import org.apache.spark.sql.SparkSession

/**
 * Used to write log files that represent batch commit points in structured streaming.
  * 用于编写代表结构化流中的批量提交点的日志文件
 * A commit log file will be written immediately after the successful completion of a
 * batch, and before processing the next batch. Here is an execution summary:
  * 提交日志文件将在成功完成批处理之后立即写入,并在处理下一批之前立即写入,这是一个执行摘要：
 * - trigger batch 1 触发批次1
 * - obtain batch 1 offsets and write to offset log 获得批1偏移量并写入偏移量日志
 * - process batch 1 处理批次1
 * - write batch 1 to completion log 写批1到完成日志
 * - trigger batch 2 trigger batch 2
 * - obtain batch 2 offsets and write to offset log 获取批次2偏移量并写入偏移量日志
 * - process batch 2
 * - write batch 2 to completion log 将批次2写入完成日志
 * ....
 *
 * The current format of the batch completion log is:
  * 批处理完成日志的当前格式是：
 * line 1: version
 * line 2: metadata (optional json string)
 */
class BatchCommitLog(sparkSession: SparkSession, path: String)
  extends HDFSMetadataLog[String](sparkSession, path) {

  import BatchCommitLog._

  def add(batchId: Long): Unit = {
    super.add(batchId, EMPTY_JSON)
  }

  override def add(batchId: Long, metadata: String): Boolean = {
    throw new UnsupportedOperationException(
      "BatchCommitLog does not take any metadata, use 'add(batchId)' instead")
  }

  override protected def deserialize(in: InputStream): String = {
    // called inside a try-finally where the underlying stream is closed in the caller
    //在调用程序中关闭底层流的try-finally内调用
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file in the offset commit log")
    }
    parseVersion(lines.next.trim, VERSION)
    EMPTY_JSON
  }

  override protected def serialize(metadata: String, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    //在调用程序中关闭底层流的try-finally内调用
    out.write(s"v${VERSION}".getBytes(UTF_8))
    out.write('\n')

    // write metadata
    out.write(EMPTY_JSON.getBytes(UTF_8))
  }
}

object BatchCommitLog {
  private val VERSION = 1
  private val EMPTY_JSON = "{}"
}

