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

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.util.Utils

/**
 * User specified options for file streams.
  * 用户指定的文件流选项
 */
class FileStreamOptions(parameters: CaseInsensitiveMap[String]) extends Logging {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val maxFilesPerTrigger: Option[Int] = parameters.get("maxFilesPerTrigger").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'maxFilesPerTrigger', must be a positive integer")
    }
  }

  /**
   * Maximum age of a file that can be found in this directory, before it is ignored. For the
   * first batch all files will be considered valid. If `latestFirst` is set to `true` and
   * `maxFilesPerTrigger` is set, then this parameter will be ignored, because old files that are
   * valid, and should be processed, may be ignored. Please refer to SPARK-19813 for details.
    *
   * 在忽略之前,可以在该目录中找到的文件的最大时间,对于第一批,所有文件将被视为有效。
    * 如果`latestFirst`被设置为`true`并且`maxFilesPerTrigger`被设置,那么这个参数将被忽略,因为有效并且应该被处理的旧文件可能被忽略。 详情请参考SPARK-19813。
   * The max age is specified with respect to the timestamp of the latest file, and not the
   * timestamp of the current system. That this means if the last file has timestamp 1000, and the
   * current system time is 2000, and max age is 200, the system will purge files older than
   * 800 (rather than 1800) from the internal state.
   * 最大时间是根据最新文件的时间戳而不是当前系统的时间戳来指定的,这意味着如果最后一个文件的时间戳为1000,
    * 当前系统时间为2000,最大时间为200,则系统将从内部状态中清除超过800（而不是1800）的文件。
   * Default to a week. 默认为一周
   */
  val maxFileAgeMs: Long =
    Utils.timeStringAsMs(parameters.getOrElse("maxFileAge", "7d"))

  /** Options as specified by the user, in a case-insensitive map, without "path" set.
    * 用户指定的选项,在不区分大小写的映射中,不设置“路径”*/
  val optionMapWithoutPath: Map[String, String] =
    parameters.filterKeys(_ != "path")

  /**
   * Whether to scan latest files first. If it's true, when the source finds unprocessed files in a
   * trigger, it will first process the latest files.
    * 是否要先扫描最新的文件,如果这是真的,当源在触发器中发现未处理的文件时,它将首先处理最新的文件。
   */
  val latestFirst: Boolean = withBooleanParameter("latestFirst", false)

  /**
   * Whether to check new files based on only the filename instead of on the full path.
   * 是否仅基于文件名而不是完整路径来检查新文件。
   * With this set to `true`, the following files would be considered as the same file, because
   * their filenames, "dataset.txt", are the same:
    * 将此设置为“true”,以下文件将被视为相同的文件,因为它们的文件名“dataset.txt”是相同的：
   * - "file:///dataset.txt"
   * - "s3://a/dataset.txt"
   * - "s3n://a/b/dataset.txt"
   * - "s3a://a/b/c/dataset.txt"
   */
  val fileNameOnly: Boolean = withBooleanParameter("fileNameOnly", false)

  private def withBooleanParameter(name: String, default: Boolean) = {
    parameters.get(name).map { str =>
      try {
        str.toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Invalid value '$str' for option '$name', must be 'true' or 'false'")
      }
    }.getOrElse(default)
  }
}
