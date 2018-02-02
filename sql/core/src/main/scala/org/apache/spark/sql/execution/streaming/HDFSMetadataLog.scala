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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.{ConcurrentModificationException, EnumSet, UUID}

import scala.reflect.ClassTag

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * A [[MetadataLog]] implementation based on HDFS. [[HDFSMetadataLog]] uses the specified `path`
 * as the metadata storage.
 * 基于HDFS的[[MetadataLog]]实现,[[HDFSMetadataLog]]使用指定的`path`作为元数据存储
  *
 * When writing a new batch, [[HDFSMetadataLog]] will firstly write to a temp file and then rename
 * it to the final batch file. If the rename step fails, there must be multiple writers and only
 * one of them will succeed and the others will fail.
  *
  * 当写入新的批处理时,[[HDFSMetadataLog]]将首先写入临时文件,然后将其重命名为最终的批处理文件,
  * 如果重命名步骤失败,则必须有多个作者,其中只有一个成功,其他将失败。
 *
 * Note: [[HDFSMetadataLog]] doesn't support S3-like file systems as they don't guarantee listing
 * files in a directory always shows the latest files.
 */
class HDFSMetadataLog[T <: AnyRef : ClassTag](sparkSession: SparkSession, path: String)
  extends MetadataLog[T] with Logging {

  private implicit val formats = Serialization.formats(NoTypeHints)

  /** Needed to serialize type T into JSON when using Jackson
    * 使用Jackson时,需要将类型T序列化为JSON*/
  private implicit val manifest = Manifest.classType[T](implicitly[ClassTag[T]].runtimeClass)

  // Avoid serializing generic sequences, see SPARK-17372
  //避免序列化通用序列,参见SPARK-17372
  require(implicitly[ClassTag[T]].runtimeClass != classOf[Seq[_]],
    "Should not create a log with type Seq, use Arrays instead - see SPARK-17372")

  import HDFSMetadataLog._

  val metadataPath = new Path(path)
  protected val fileManager = createFileManager()

  if (!fileManager.exists(metadataPath)) {
    fileManager.mkdirs(metadataPath)
  }

  /**
   * A `PathFilter` to filter only batch files
    * `PathFilter`只过滤批处理文件
   */
  protected val batchFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = isBatchFile(path)
  }

  protected def batchIdToPath(batchId: Long): Path = {
    new Path(metadataPath, batchId.toString)
  }

  protected def pathToBatchId(path: Path) = {
    path.getName.toLong
  }

  protected def isBatchFile(path: Path) = {
    try {
      path.getName.toLong
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  protected def serialize(metadata: T, out: OutputStream): Unit = {
    // called inside a try-finally where the underlying stream is closed in the caller
    //在调用程序中关闭底层流的try-finally内调用
    Serialization.write(metadata, out)
  }

  protected def deserialize(in: InputStream): T = {
    // called inside a try-finally where the underlying stream is closed in the caller
    //在调用程序中关闭底层流的try-finally内调用
    val reader = new InputStreamReader(in, StandardCharsets.UTF_8)
    Serialization.read[T](reader)
  }

  /**
   * Store the metadata for the specified batchId and return `true` if successful. If the batchId's
   * metadata has already been stored, this method will return `false`.
    * 存储指定的batchId的元数据,如果成功则返回“true”,如果batchId的元数据已经被存储了,这个方法将会返回`false`。
   */
  override def add(batchId: Long, metadata: T): Boolean = {
    require(metadata != null, "'null' metadata cannot written to a metadata log")
    get(batchId).map(_ => false).getOrElse {
      // Only write metadata when the batch has not yet been written
      //只有在批量尚未写入时才写入元数据
      writeBatch(batchId, metadata)
      true
    }
  }

  private def writeTempBatch(metadata: T): Option[Path] = {
    while (true) {
      val tempPath = new Path(metadataPath, s".${UUID.randomUUID.toString}.tmp")
      try {
        val output = fileManager.create(tempPath)
        try {
          serialize(metadata, output)
          return Some(tempPath)
        } finally {
          output.close()
        }
      } catch {
        case e: FileAlreadyExistsException =>
          // Failed to create "tempPath". There are two cases:
          // 1. Someone is creating "tempPath" too.
          // 2. This is a restart. "tempPath" has already been created but not moved to the final
          // batch file (not committed).
          //
          // For both cases, the batch has not yet been committed. So we can retry it.
          //
          // Note: there is a potential risk here: if HDFSMetadataLog A is running, people can use
          // the same metadata path to create "HDFSMetadataLog" and fail A. However, this is not a
          // big problem because it requires the attacker must have the permission to write the
          // metadata path. In addition, the old Streaming also have this issue, people can create
          // malicious checkpoint files to crash a Streaming application too.
      }
    }
    None
  }

  /**
   * Write a batch to a temp file then rename it to the batch file.
   * 将批处理写入临时文件,然后将其重命名为批处理文件
    *
   * There may be multiple [[HDFSMetadataLog]] using the same metadata path. Although it is not a
   * valid behavior, we still need to prevent it from destroying the files.
    * 可能有多个[[HDFSMetadataLog]]使用相同的元数据路径,虽然这不是一个有效的行为,但我们仍然需要防止它破坏文件
   */
  private def writeBatch(batchId: Long, metadata: T): Unit = {
    val tempPath = writeTempBatch(metadata).getOrElse(
      throw new IllegalStateException(s"Unable to create temp batch file $batchId"))
    try {
      // Try to commit the batch 尝试提交批处理
      // It will fail if there is an existing file (someone has committed the batch)
      //如果有一个现有的文件（有人已经提交了批处理）
      logDebug(s"Attempting to write log #${batchIdToPath(batchId)}")
      fileManager.rename(tempPath, batchIdToPath(batchId))

      // SPARK-17475: HDFSMetadataLog should not leak CRC files
      // If the underlying filesystem didn't rename the CRC file, delete it.
      //如果底层文件系统没有重命名CRC文件,请将其删除
      val crcPath = new Path(tempPath.getParent(), s".${tempPath.getName()}.crc")
      if (fileManager.exists(crcPath)) fileManager.delete(crcPath)
    } catch {
      case e: FileAlreadyExistsException =>
        // If "rename" fails, it means some other "HDFSMetadataLog" has committed the batch.
        //如果“重命名”失败，则意味着其他一些“HDFSMetadataLog”已经提交了批处理
        // So throw an exception to tell the user this is not a valid behavior.
        //所以抛出一个异常来告诉用户这不是一个有效的行为
        throw new ConcurrentModificationException(
          s"Multiple HDFSMetadataLog are using $path", e)
    } finally {
      fileManager.delete(tempPath)
    }
  }

  /**
   * @return the deserialized metadata in a batch file, or None if file not exist.
    *         批处理文件中反序列化的元数据,如果文件不存在,则返回None
   * @throws IllegalArgumentException when path does not point to a batch file.
   */
  def get(batchFile: Path): Option[T] = {
    if (fileManager.exists(batchFile)) {
      if (isBatchFile(batchFile)) {
        get(pathToBatchId(batchFile))
      } else {
        throw new IllegalArgumentException(s"File ${batchFile} is not a batch file!")
      }
    } else {
      None
    }
  }

  override def get(batchId: Long): Option[T] = {
    val batchMetadataFile = batchIdToPath(batchId)
    if (fileManager.exists(batchMetadataFile)) {
      val input = fileManager.open(batchMetadataFile)
      try {
        Some(deserialize(input))
      } catch {
        case ise: IllegalStateException =>
          // re-throw the exception with the log file path added
          //用添加的日志文件路径重新抛出异常
          throw new IllegalStateException(
            s"Failed to read log file $batchMetadataFile. ${ise.getMessage}", ise)
      } finally {
        IOUtils.closeQuietly(input)
      }
    } else {
      logDebug(s"Unable to find batch $batchMetadataFile")
      None
    }
  }

  override def get(startId: Option[Long], endId: Option[Long]): Array[(Long, T)] = {
    assert(startId.isEmpty || endId.isEmpty || startId.get <= endId.get)
    val files = fileManager.list(metadataPath, batchFilesFilter)
    val batchIds = files
      .map(f => pathToBatchId(f.getPath))
      .filter { batchId =>
        (endId.isEmpty || batchId <= endId.get) && (startId.isEmpty || batchId >= startId.get)
    }.sorted

    verifyBatchIds(batchIds, startId, endId)

    batchIds.map(batchId => (batchId, get(batchId))).filter(_._2.isDefined).map {
      case (batchId, metadataOption) =>
        (batchId, metadataOption.get)
    }
  }

  override def getLatest(): Option[(Long, T)] = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath))
      .sorted
      .reverse
    for (batchId <- batchIds) {
      val batch = get(batchId)
      if (batch.isDefined) {
        return Some((batchId, batch.get))
      }
    }
    None
  }

  /**
   * Get an array of [FileStatus] referencing batch files.
   * The array is sorted by most recent batch file first to
   * oldest batch file.
    * 获取引用批处理文件的[FileStatus]数组,该数组按最近的批处理文件首先到最旧的批处理文件进行排序
   */
  def getOrderedBatchFiles(): Array[FileStatus] = {
    fileManager.list(metadataPath, batchFilesFilter)
      .sortBy(f => pathToBatchId(f.getPath))
      .reverse
  }

  /**
   * Removes all the log entry earlier than thresholdBatchId (exclusive).
    * 删除thresholdBatchId(独占)之前的所有日志条目
   */
  override def purge(thresholdBatchId: Long): Unit = {
    val batchIds = fileManager.list(metadataPath, batchFilesFilter)
      .map(f => pathToBatchId(f.getPath))

    for (batchId <- batchIds if batchId < thresholdBatchId) {
      val path = batchIdToPath(batchId)
      fileManager.delete(path)
      logTrace(s"Removed metadata log file: $path")
    }
  }

  private def createFileManager(): FileManager = {
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    try {
      new FileContextManager(metadataPath, hadoopConf)
    } catch {
      case e: UnsupportedFileSystemException =>
        logWarning("Could not use FileContext API for managing metadata log files at path " +
          s"$metadataPath. Using FileSystem API instead for managing log files. The log may be " +
          s"inconsistent under failures.")
        new FileSystemManager(metadataPath, hadoopConf)
    }
  }

  /**
   * Parse the log version from the given `text` -- will throw exception when the parsed version
   * exceeds `maxSupportedVersion`, or when `text` is malformed (such as "xyz", "v", "v-1",
   * "v123xyz" etc.)
    * 解析来自给定`text`的日志版本 - 当解析的版本超过maxSupportedVersion时,
    * 或者当文本格式不正确时（例如“xyz”，“v”，“v-1”，“v123xyz “等）
   */
  private[sql] def parseVersion(text: String, maxSupportedVersion: Int): Int = {
    if (text.length > 0 && text(0) == 'v') {
      val version =
        try {
          text.substring(1, text.length).toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
              s"version from $text.")
        }
      if (version > 0) {
        if (version > maxSupportedVersion) {
          throw new IllegalStateException(s"UnsupportedLogVersion: maximum supported log version " +
            s"is v${maxSupportedVersion}, but encountered v$version. The log file was produced " +
            s"by a newer version of Spark and cannot be read by this version. Please upgrade.")
        } else {
          return version
        }
      }
    }

    // reaching here means we failed to read the correct log version
    //到达这里意味着我们没有阅读正确的日志版本
    throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
      s"version from $text.")
  }
}

object HDFSMetadataLog {

  /** A simple trait to abstract out the file management operations needed by HDFSMetadataLog.
    * 一个简单的特征来抽象出HDFSMetadataLog所需的文件管理操作 */
  trait FileManager {

    /** List the files in a path that matches a filter.
      * 列出与过滤器匹配的路径中的文件*/
    def list(path: Path, filter: PathFilter): Array[FileStatus]

    /** Make directory at the give path and all its parent directories as needed.
      * 根据需要在给定路径及其所有父目录中创建目录*/
    def mkdirs(path: Path): Unit

    /** Whether path exists 是否存在路径*/
    def exists(path: Path): Boolean

    /** Open a file for reading, or throw exception if it does not exist.
      * 打开一个文件读取，或抛出异常，如果它不存在。*/
    def open(path: Path): FSDataInputStream

    /** Create path, or throw exception if it already exists
      * 创建路径,或抛出异常,如果它已经存在*/
    def create(path: Path): FSDataOutputStream

    /**
     * Atomically rename path, or throw exception if it cannot be done.
      * 原子重命名路径,或抛出异常,如果不能完成
     * Should throw FileNotFoundException if srcPath does not exist.
      * 如果srcPath不存在，应抛出FileNotFoundException。
     * Should throw FileAlreadyExistsException if destPath already exists.
      * 如果destPath已经存在,应该抛出FileAlreadyExistsException
     */
    def rename(srcPath: Path, destPath: Path): Unit

    /** Recursively delete a path if it exists. Should not throw exception if file doesn't exist.
      * 递归删除路径,如果它存在,如果文件不存在,不应该抛出异常*/
    def delete(path: Path): Unit
  }

  /**
   * Default implementation of FileManager using newer FileContext API.
    * 使用较新的FileContext API的FileManager的默认实现
   */
  class FileContextManager(path: Path, hadoopConf: Configuration) extends FileManager {
    private val fc = if (path.toUri.getScheme == null) {
      FileContext.getFileContext(hadoopConf)
    } else {
      FileContext.getFileContext(path.toUri, hadoopConf)
    }

    override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
      fc.util.listStatus(path, filter)
    }

    override def rename(srcPath: Path, destPath: Path): Unit = {
      fc.rename(srcPath, destPath)
    }

    override def mkdirs(path: Path): Unit = {
      fc.mkdir(path, FsPermission.getDirDefault, true)
    }

    override def open(path: Path): FSDataInputStream = {
      fc.open(path)
    }

    override def create(path: Path): FSDataOutputStream = {
      fc.create(path, EnumSet.of(CreateFlag.CREATE))
    }

    override def exists(path: Path): Boolean = {
      fc.util().exists(path)
    }

    override def delete(path: Path): Unit = {
      try {
        fc.delete(path, true)
      } catch {
        case e: FileNotFoundException =>
        // ignore if file has already been deleted
      }
    }
  }

  /**
   * Implementation of FileManager using older FileSystem API. Note that this implementation
   * cannot provide atomic renaming of paths, hence can lead to consistency issues. This
   * should be used only as a backup option, when FileContextManager cannot be used.
   */
  class FileSystemManager(path: Path, hadoopConf: Configuration) extends FileManager {
    private val fs = path.getFileSystem(hadoopConf)

    override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
      fs.listStatus(path, filter)
    }

    /**
     * Rename a path. Note that this implementation is not atomic.
      * 重命名路径,请注意,这个实现不是原子的
     * @throws FileNotFoundException if source path does not exist.
      *                               如果源路径不存在
     * @throws FileAlreadyExistsException if destination path already exists.
      *                                    如果目标路径已经存在
     * @throws IOException if renaming fails for some unknown reason.
      *                     如果重命名失败,原因不明
     */
    override def rename(srcPath: Path, destPath: Path): Unit = {
      if (!fs.exists(srcPath)) {
        throw new FileNotFoundException(s"Source path does not exist: $srcPath")
      }
      if (fs.exists(destPath)) {
        throw new FileAlreadyExistsException(s"Destination path already exists: $destPath")
      }
      if (!fs.rename(srcPath, destPath)) {
        throw new IOException(s"Failed to rename $srcPath to $destPath")
      }
    }

    override def mkdirs(path: Path): Unit = {
      fs.mkdirs(path, FsPermission.getDirDefault)
    }

    override def open(path: Path): FSDataInputStream = {
      fs.open(path)
    }

    override def create(path: Path): FSDataOutputStream = {
      fs.create(path, false)
    }

    override def exists(path: Path): Boolean = {
      fs.exists(path)
    }

    override def delete(path: Path): Unit = {
      try {
        fs.delete(path, true)
      } catch {
        case e: FileNotFoundException =>
          // ignore if file has already been deleted
      }
    }
  }

  /**
   * Verify if batchIds are continuous and between `startId` and `endId`.
   * 验证batchIds是否连续,并在`startEd`和`endEd`之间。
   * @param batchIds the sorted ids to verify.排序的ID来验证
   * @param startId the start id. If it's set, batchIds should start with this id.
    *                起始ID,如果设置了,batchIds应该以这个ID开头
   * @param endId the start id. If it's set, batchIds should end with this id.
    *              起始ID,如果设置了,batchIds应该以这个ID结束
   */
  def verifyBatchIds(batchIds: Seq[Long], startId: Option[Long], endId: Option[Long]): Unit = {
    // Verify that we can get all batches between `startId` and `endId`.
    //验证我们可以在`startEd`和`endEd`之间获得所有批次
    if (startId.isDefined || endId.isDefined) {
      if (batchIds.isEmpty) {
        throw new IllegalStateException(s"batch ${startId.orElse(endId).get} doesn't exist")
      }
      if (startId.isDefined) {
        val minBatchId = batchIds.head
        assert(minBatchId >= startId.get)
        if (minBatchId != startId.get) {
          val missingBatchIds = startId.get to minBatchId
          throw new IllegalStateException(
            s"batches (${missingBatchIds.mkString(", ")}) don't exist " +
              s"(startId: $startId, endId: $endId)")
        }
      }

      if (endId.isDefined) {
        val maxBatchId = batchIds.last
        assert(maxBatchId <= endId.get)
        if (maxBatchId != endId.get) {
          val missingBatchIds = maxBatchId to endId.get
          throw new IllegalStateException(
            s"batches (${missingBatchIds.mkString(", ")}) don't  exist " +
              s"(startId: $startId, endId: $endId)")
        }
      }
    }

    if (batchIds.nonEmpty) {
      val minBatchId = batchIds.head
      val maxBatchId = batchIds.last
      val missingBatchIds = (minBatchId to maxBatchId).toSet -- batchIds
      if (missingBatchIds.nonEmpty) {
        throw new IllegalStateException(s"batches (${missingBatchIds.mkString(", ")}) " +
          s"don't exist (startId: $startId, endId: $endId)")
      }
    }
  }
}
