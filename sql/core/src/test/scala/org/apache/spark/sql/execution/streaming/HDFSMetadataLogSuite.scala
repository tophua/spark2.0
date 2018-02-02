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

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI
import java.util.ConcurrentModificationException

import scala.language.implicitConversions
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.scalatest.concurrent.AsyncAssertions._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog.{FileContextManager, FileManager, FileSystemManager}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.UninterruptibleThread
//HDFS的元数据日志套件
class HDFSMetadataLogSuite extends SparkFunSuite with SharedSQLContext {

  /** To avoid caching of FS objects
    * 避免FS对象的高速缓存*/
  override protected def sparkConf =
    super.sparkConf.set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")

  private implicit def toOption[A](a: A): Option[A] = Option(a)
  //文件管理器：文件上下文管理器
  test("FileManager: FileContextManager") {
    withTempDir { temp =>
      val path = new Path(temp.getAbsolutePath)
      testFileManager(path, new FileContextManager(path, new Configuration))
    }
  }
  //文件管理器：文件系统管理器
  test("FileManager: FileSystemManager") {
    withTempDir { temp =>
      val path = new Path(temp.getAbsolutePath)
      testFileManager(path, new FileSystemManager(path, new Configuration))
    }
  }
  //HDFS元数据日志：基本
  test("HDFSMetadataLog: basic") {
    withTempDir { temp =>
      //使用不存在的目录来测试日志是否生成目录
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
      val metadataLog = new HDFSMetadataLog[String](spark, dir.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(None, Some(0)) === Array(0 -> "batch0"))

      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))

      // Adding the same batch does nothing
      metadataLog.add(1, "batch1-duplicated")
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }
  //HDFSMetadataLog：从FileContext回退到FileSystem
  testQuietly("HDFSMetadataLog: fallback from FileContext to FileSystem") {
    spark.conf.set(
      s"fs.$scheme.impl",
      classOf[FakeFileSystem].getName)
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, s"$scheme://${temp.toURI.getPath}")
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(None, Some(0)) === Array(0 -> "batch0"))


      val metadataLog2 = new HDFSMetadataLog[String](spark, s"$scheme://${temp.toURI.getPath}")
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog2.get(None, Some(0)) === Array(0 -> "batch0"))

    }
  }
  //HDFSMetadataLog：清除
  test("HDFSMetadataLog: purge") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.add(2, "batch2"))
      assert(metadataLog.get(0).isDefined)
      assert(metadataLog.get(1).isDefined)
      assert(metadataLog.get(2).isDefined)
      assert(metadataLog.getLatest().get._1 == 2)

      metadataLog.purge(2)
      assert(metadataLog.get(0).isEmpty)
      assert(metadataLog.get(1).isEmpty)
      assert(metadataLog.get(2).isDefined)
      assert(metadataLog.getLatest().get._1 == 2)

      // There should be exactly one file, called "2", in the metadata directory.
      //元数据目录中应该只有一个名为“2”的文件
      // This check also tests for regressions of SPARK-17475
      val allFiles = new File(metadataLog.metadataPath.toString).listFiles().toSeq
      assert(allFiles.size == 1)
      assert(allFiles(0).getName() == "2")
    }
  }
  //
  test("HDFSMetadataLog: parseVersion") {
    withTempDir { dir =>
      val metadataLog = new HDFSMetadataLog[String](spark, dir.getAbsolutePath)
      def assertLogFileMalformed(func: => Int): Unit = {
        val e = intercept[IllegalStateException] { func }
        assert(e.getMessage.contains(s"Log file was malformed: failed to read correct log version"))
      }
      assertLogFileMalformed { metadataLog.parseVersion("", 100) }
      assertLogFileMalformed { metadataLog.parseVersion("xyz", 100) }
      assertLogFileMalformed { metadataLog.parseVersion("v10.x", 100) }
      assertLogFileMalformed { metadataLog.parseVersion("10", 100) }
      assertLogFileMalformed { metadataLog.parseVersion("v0", 100) }
      assertLogFileMalformed { metadataLog.parseVersion("v-10", 100) }

      assert(metadataLog.parseVersion("v10", 10) === 10)
      assert(metadataLog.parseVersion("v10", 100) === 10)

      val e = intercept[IllegalStateException] { metadataLog.parseVersion("v200", 100) }
      Seq(
        "maximum supported log version is v100, but encountered v200",
        "produced by a newer version of Spark and cannot be read by this version"
      ).foreach { message =>
        assert(e.getMessage.contains(message))
      }
    }
  }
  //HDFSMetadataLog：重启
  test("HDFSMetadataLog: restart") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))

      val metadataLog2 = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.get(1) === Some("batch1"))
      assert(metadataLog2.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog2.get(None, Some(1)) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }
  //HDFSMetadataLog：元数据目录冲突
  test("HDFSMetadataLog: metadata directory collision") {
    withTempDir { temp =>
      val waiter = new Waiter
      val maxBatchId = 100
      for (id <- 0 until 10) {
        new UninterruptibleThread(s"HDFSMetadataLog: metadata directory collision - thread $id") {
          override def run(): Unit = waiter {
            val metadataLog =
              new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
            try {
              var nextBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)
              nextBatchId += 1
              while (nextBatchId <= maxBatchId) {
                metadataLog.add(nextBatchId, nextBatchId.toString)
                nextBatchId += 1
              }
            } catch {
              case e: ConcurrentModificationException =>
              // This is expected since there are multiple writers
              //这是预期的,因为有多个写
            } finally {
              waiter.dismiss()
            }
          }
        }.start()
      }

      waiter.await(timeout(10.seconds), dismissals(10))
      val metadataLog = new HDFSMetadataLog[String](spark, temp.getAbsolutePath)
      assert(metadataLog.getLatest() === Some(maxBatchId -> maxBatchId.toString))
      assert(
        metadataLog.get(None, Some(maxBatchId)) === (0 to maxBatchId).map(i => (i, i.toString)))
    }
  }

  /** Basic test case for [[FileManager]] implementation.
    * [[FileManager]]实现的基本测试用例 */
  private def testFileManager(basePath: Path, fm: FileManager): Unit = {
    // Mkdirs
    val dir = new Path(s"$basePath/dir/subdir/subsubdir")
    assert(!fm.exists(dir))
    fm.mkdirs(dir)
    assert(fm.exists(dir))
    fm.mkdirs(dir)

    // List
    val acceptAllFilter = new PathFilter {
      override def accept(path: Path): Boolean = true
    }
    val rejectAllFilter = new PathFilter {
      override def accept(path: Path): Boolean = false
    }
    assert(fm.list(basePath, acceptAllFilter).exists(_.getPath.getName == "dir"))
    assert(fm.list(basePath, rejectAllFilter).length === 0)

    // Create 创建
    val path = new Path(s"$dir/file")
    assert(!fm.exists(path))
    fm.create(path).close()
    assert(fm.exists(path))
    intercept[IOException] {
      fm.create(path)
    }

    // Open and delete 打开并删除
    fm.open(path).close()
    fm.delete(path)
    assert(!fm.exists(path))
    intercept[IOException] {
      fm.open(path)
    }
    fm.delete(path) // should not throw exception

    // Rename
    val path1 = new Path(s"$dir/file1")
    val path2 = new Path(s"$dir/file2")
    fm.create(path1).close()
    assert(fm.exists(path1))
    fm.rename(path1, path2)
    intercept[FileNotFoundException] {
      fm.rename(path1, path2)
    }
    val path3 = new Path(s"$dir/file3")
    fm.create(path3).close()
    assert(fm.exists(path3))
    intercept[FileAlreadyExistsException] {
      fm.rename(path2, path3)
    }
  }
  //验证批次ID
  test("verifyBatchIds") {
    import HDFSMetadataLog.verifyBatchIds
    verifyBatchIds(Seq(1L, 2L, 3L), Some(1L), Some(3L))
    verifyBatchIds(Seq(1L), Some(1L), Some(1L))
    verifyBatchIds(Seq(1L, 2L, 3L), None, Some(3L))
    verifyBatchIds(Seq(1L, 2L, 3L), Some(1L), None)
    verifyBatchIds(Seq(1L, 2L, 3L), None, None)

    intercept[IllegalStateException](verifyBatchIds(Seq(), Some(1L), None))
    intercept[IllegalStateException](verifyBatchIds(Seq(), None, Some(1L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(), Some(1L), Some(1L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(2, 3, 4), Some(1L), None))
    intercept[IllegalStateException](verifyBatchIds(Seq(2, 3, 4), None, Some(5L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(2, 3, 4), Some(1L), Some(5L)))
    intercept[IllegalStateException](verifyBatchIds(Seq(1, 2, 4, 5), Some(1L), Some(5L)))
  }
}

/** FakeFileSystem to test fallback of the HDFSMetadataLog from FileContext to FileSystem API
  * FakeFileSystem测试从FileContext到FileSystem API的HDFSMetadataLog的回退*/
class FakeFileSystem extends RawLocalFileSystem {
  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }
}

object FakeFileSystem {
  val scheme = s"HDFSMetadataLogSuite${math.abs(Random.nextInt)}"
}
