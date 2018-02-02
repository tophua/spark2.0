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

import java.io.{InputStream, IOException, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import scala.io.{Source => IOSource}
import scala.reflect.ClassTag

import org.apache.hadoop.fs.{Path, PathFilter}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession

/**
 * An abstract class for compactible metadata logs. It will write one log file for each batch.
  * 可压缩元数据日志的抽象类,它会为每个批次写入一个日志文件
 * The first line of the log file is the version number, and there are multiple serialized
 * metadata lines following.
  * 日志文件的第一行是版本号,下面有多个序列化的元数据行
 *
 * As reading from many small files is usually pretty slow, also too many
 * small files in one folder will mess the FS, [[CompactibleFileStreamLog]] will
 * compact log files every 10 batches by default into a big file. When
 * doing a compaction, it will read all old log files and merge them with the new batch.
  * 由于很多小文件的读取速度通常很慢,一个文件夹中的太多小文件也会混淆FS,
  * [CompactibleFileStreamLog]会将每10个批次的日志文件默认压缩成一个大文件。 在进行压缩时，它将读取所有旧的日志文件，并将它们与新批次合并。
 */
abstract class CompactibleFileStreamLog[T <: AnyRef : ClassTag](
    metadataLogVersion: Int,
    sparkSession: SparkSession,
    path: String)
  extends HDFSMetadataLog[Array[T]](sparkSession, path) {

  import CompactibleFileStreamLog._

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson
    * 使用Jackson时,需要将类型T序列化为JSON */
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  protected val minBatchesToRetain = sparkSession.sessionState.conf.minBatchesToRetain

  /**
   * If we delete the old files after compaction at once, there is a race condition in S3: other
   * processes may see the old files are deleted but still cannot see the compaction file using
   * "list". The `allFiles` handles this by looking for the next compaction file directly, however,
   * a live lock may happen if the compaction happens too frequently: one processing keeps deleting
   * old files while another one keeps retrying. Setting a reasonable cleanup delay could avoid it.
    * 如果一次压缩后删除旧文件,则S3中存在竞态条件：其他进程可能会看到旧文件被删除，但仍然无法使用“list”查看压缩文件,
    * `allFiles`通过直接查找下一个压缩文件来处理这个问题,但是,如果压缩发生得太频繁,
    * 则可能会发生活动锁定：一个处理会保留删除旧文件而另一个持续重试,设置一个合理的清理延迟可以避免它。
   */
  protected def fileCleanupDelayMs: Long

  protected def isDeletingExpiredLog: Boolean

  protected def defaultCompactInterval: Int

  protected final lazy val compactInterval: Int = {
    // SPARK-18187: "compactInterval" can be set by user via defaultCompactInterval.
    // If there are existing log entries, then we should ensure a compatible compactInterval
    // is used, irrespective of the defaultCompactInterval. There are three cases:
    //
    // 1. If there is no '.compact' file, we can use the default setting directly.
    // 2. If there are two or more '.compact' files, we use the interval of patch id suffix with
    // '.compact' as compactInterval. This case could arise if isDeletingExpiredLog == false.
    // 3. If there is only one '.compact' file, then we must find a compact interval
    // that is compatible with (i.e., a divisor of) the previous compact file, and that
    // faithfully tries to represent the revised default compact interval i.e., is at least
    // is large if possible.
    // e.g., if defaultCompactInterval is 5 (and previous compact interval could have
    // been any 2,3,4,6,12), then a log could be: 11.compact, 12, 13, in which case
    // will ensure that the new compactInterval = 6 > 5 and (11 + 1) % 6 == 0
    val compactibleBatchIds = fileManager.list(metadataPath, batchFilesFilter)
      .filter(f => f.getPath.toString.endsWith(CompactibleFileStreamLog.COMPACT_FILE_SUFFIX))
      .map(f => pathToBatchId(f.getPath))
      .sorted
      .reverse

    // Case 1
    var interval = defaultCompactInterval
    if (compactibleBatchIds.length >= 2) {
      // Case 2
      val latestCompactBatchId = compactibleBatchIds(0)
      val previousCompactBatchId = compactibleBatchIds(1)
      interval = (latestCompactBatchId - previousCompactBatchId).toInt
    } else if (compactibleBatchIds.length == 1) {
      // Case 3
      interval = CompactibleFileStreamLog.deriveCompactInterval(
        defaultCompactInterval, compactibleBatchIds(0).toInt)
    }
    assert(interval > 0, s"intervalValue = $interval not positive value.")
    logInfo(s"Set the compact interval to $interval " +
      s"[defaultCompactInterval: $defaultCompactInterval]")
    interval
  }

  /**
   * Filter out the obsolete logs.
    * 过滤掉过时的日志
   */
  def compactLogs(logs: Seq[T]): Seq[T]

  override def batchIdToPath(batchId: Long): Path = {
    if (isCompactionBatch(batchId, compactInterval)) {
      new Path(metadataPath, s"$batchId$COMPACT_FILE_SUFFIX")
    } else {
      new Path(metadataPath, batchId.toString)
    }
  }

  override def pathToBatchId(path: Path): Long = {
    getBatchIdFromFileName(path.getName)
  }

  override def isBatchFile(path: Path): Boolean = {
    try {
      getBatchIdFromFileName(path.getName)
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  override def serialize(logData: Array[T], out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    out.write(("v" + metadataLogVersion).getBytes(UTF_8))
    logData.foreach { data =>
      out.write('\n')
      out.write(Serialization.write(data).getBytes(UTF_8))
    }
  }

  override def deserialize(in: InputStream): Array[T] = {
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw new IllegalStateException("Incomplete log file")
    }
    val version = parseVersion(lines.next(), metadataLogVersion)
    lines.map(Serialization.read[T]).toArray
  }

  override def add(batchId: Long, logs: Array[T]): Boolean = {
    val batchAdded =
      if (isCompactionBatch(batchId, compactInterval)) {
        compact(batchId, logs)
      } else {
        super.add(batchId, logs)
      }
    if (batchAdded && isDeletingExpiredLog) {
      deleteExpiredLog(batchId)
    }
    batchAdded
  }

  /**
   * Compacts all logs before `batchId` plus the provided `logs`, and writes them into the
   * corresponding `batchId` file. It will delete expired files as well if enabled.
    * 在`batchId`加上所提供的`logs`之前压缩所有日志,并将它们写入相应的`batchId`文件中,它将删除过期的文件,如果启用
   */
  private def compact(batchId: Long, logs: Array[T]): Boolean = {
    val validBatches = getValidBatchesBeforeCompactionBatch(batchId, compactInterval)
    val allLogs = validBatches.map { id =>
      super.get(id).getOrElse {
        throw new IllegalStateException(
          s"${batchIdToPath(id)} doesn't exist when compacting batch $batchId " +
            s"(compactInterval: $compactInterval)")
      }
    }.flatten ++ logs
    // Return false as there is another writer.
    super.add(batchId, compactLogs(allLogs).toArray)
  }

  /**
   * Returns all files except the deleted ones.
    * 返回除已删除的文件之外的所有文件
   */
  def allFiles(): Array[T] = {
    var latestId = getLatest().map(_._1).getOrElse(-1L)
    // There is a race condition when `FileStreamSink` is deleting old files and `StreamFileIndex`
    // is calling this method. This loop will retry the reading to deal with the
    // race condition.
    //当“FileStreamSink”正在删除旧文件,“StreamFileIndex”正在调用此方法时,会出现竞争状态,这个循环会重试阅读来处理竞争条件。
    while (true) {
      if (latestId >= 0) {
        try {
          val logs =
            getAllValidBatches(latestId, compactInterval).map { id =>
              super.get(id).getOrElse {
                throw new IllegalStateException(
                  s"${batchIdToPath(id)} doesn't exist " +
                    s"(latestId: $latestId, compactInterval: $compactInterval)")
              }
            }.flatten
          return compactLogs(logs).toArray
        } catch {
          case e: IOException =>
            // Another process using `CompactibleFileStreamLog` may delete the batch files when
            // `StreamFileIndex` are reading. However, it only happens when a compaction is
            // deleting old files. If so, let's try the next compaction batch and we should find it.
            // Otherwise, this is a real IO issue and we should throw it.
            //另一个进程使用` compactiblefilestreamlog `可以删除批处理文件时,` streamfileindex `读。
            // 但是,只有在压缩删除旧文件时才发生,如果是这样,让我们试试下一个压实批次,我们应该找到它,否则,这是一个真正的IO问题,我们应该扔掉它。
            latestId = nextCompactionBatchId(latestId, compactInterval)
            super.get(latestId).getOrElse {
              throw e
            }
        }
      } else {
        return Array.empty
      }
    }
    Array.empty
  }

  /**
   * Delete expired log entries that proceed the currentBatchId and retain
   * sufficient minimum number of batches (given by minBatchsToRetain). This
   * equates to retaining the earliest compaction log that proceeds
   * batch id position currentBatchId + 1 - minBatchesToRetain. All log entries
   * prior to the earliest compaction log proceeding that position will be removed.
   * However, due to the eventual consistency of S3, the compaction file may not
   * be seen by other processes at once. So we only delete files created
   * `fileCleanupDelayMs` milliseconds ago.
    * 删除过期的日志条目进行currentbatchid和保留足够的最小数量（批了minbatchstoretain）,
    * 这相当于保留最早的日志进行压实批次ID位置currentbatchid + 1 minbatchestoretain,
    * 在最早的压实日志之前,所有的日志条目都会被删除,然而,
    * 由于S3的最终一致性,可能不会同时看到其他进程的压缩文件,所以我们只删除创建的文件
   */
  private def deleteExpiredLog(currentBatchId: Long): Unit = {
    if (compactInterval <= currentBatchId + 1 - minBatchesToRetain) {
      // Find the first compaction batch id that maintains minBatchesToRetain
      val minBatchId = currentBatchId + 1 - minBatchesToRetain
      val minCompactionBatchId = minBatchId - (minBatchId % compactInterval) - 1
      assert(isCompactionBatch(minCompactionBatchId, compactInterval),
        s"$minCompactionBatchId is not a compaction batch")

      logInfo(s"Current compact batch id = $currentBatchId " +
        s"min compaction batch id to delete = $minCompactionBatchId")

      val expiredTime = System.currentTimeMillis() - fileCleanupDelayMs
      fileManager.list(metadataPath, new PathFilter {
        override def accept(path: Path): Boolean = {
          try {
            val batchId = getBatchIdFromFileName(path.getName)
            batchId < minCompactionBatchId
          } catch {
            case _: NumberFormatException =>
              false
          }
        }
      }).foreach { f =>
        if (f.getModificationTime <= expiredTime) {
          fileManager.delete(f.getPath)
        }
      }
    }
  }
}

object CompactibleFileStreamLog {
  val COMPACT_FILE_SUFFIX = ".compact"

  def getBatchIdFromFileName(fileName: String): Long = {
    fileName.stripSuffix(COMPACT_FILE_SUFFIX).toLong
  }

  /**
   * Returns if this is a compaction batch. FileStreamSinkLog will compact old logs every
   * `compactInterval` commits.
    * 如果这是一个压缩批处理返回,filestreamsinklog将紧凑的旧记录每一个` compactinterval `犯
   *
   * E.g., if `compactInterval` is 3, then 2, 5, 8, ... are all compaction batches.
   */
  def isCompactionBatch(batchId: Long, compactInterval: Int): Boolean = {
    (batchId + 1) % compactInterval == 0
  }

  /**
   * Returns all valid batches before the specified `compactionBatchId`. They contain all logs we
   * need to do a new compaction.
   * 返回所有有效的批次在指定的` compactionbatchid `,它们包含所有需要进行新的压缩的日志
   * E.g., if `compactInterval` is 3 and `compactionBatchId` is 5, this method should returns
   * `Seq(2, 3, 4)` (Note: it includes the previous compaction batch 2).
   */
  def getValidBatchesBeforeCompactionBatch(
      compactionBatchId: Long,
      compactInterval: Int): Seq[Long] = {
    assert(isCompactionBatch(compactionBatchId, compactInterval),
      s"$compactionBatchId is not a compaction batch")
    (math.max(0, compactionBatchId - compactInterval)) until compactionBatchId
  }

  /**
   * Returns all necessary logs before `batchId` (inclusive). If `batchId` is a compaction, just
   * return itself. Otherwise, it will find the previous compaction batch and return all batches
   * between it and `batchId`.
    * 返回所有必要的日志之前` batchid `（含）,如果` batchid `是压实,
    * 只是回归本身,否则,它会找到以前的压实批返回所有批次之间,` batchid `。
   */
  def getAllValidBatches(batchId: Long, compactInterval: Long): Seq[Long] = {
    assert(batchId >= 0)
    val start = math.max(0, (batchId + 1) / compactInterval * compactInterval - 1)
    start to batchId
  }

  /**
   * Returns the next compaction batch id after `batchId`.
    * 返回下一个压实后` batchid `批次ID。
   */
  def nextCompactionBatchId(batchId: Long, compactInterval: Long): Long = {
    (batchId + compactInterval + 1) / compactInterval * compactInterval - 1
  }

  /**
   * Derives a compact interval from the latest compact batch id and
   * a default compact interval.
    * 从最新的紧凑批处理ID和默认的紧凑间隔派生一个紧凑的间隔
   */
  def deriveCompactInterval(defaultInterval: Int, latestCompactBatchId: Int) : Int = {
    if (latestCompactBatchId + 1 <= defaultInterval) {
      latestCompactBatchId + 1
    } else if (defaultInterval < (latestCompactBatchId + 1) / 2) {
      // Find the first divisor >= default compact interval
      //求第一个除数>缺省紧区间
      def properDivisors(min: Int, n: Int) =
        (min to n/2).view.filter(i => n % i == 0) :+ n

      properDivisors(defaultInterval, latestCompactBatchId + 1).head
    } else {
      // default compact interval > than any divisor other than latest compact id
      //默认的紧凑间隔>比最近的紧凑ID以外的任何除数
      latestCompactBatchId + 1
    }
  }
}

