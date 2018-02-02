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
import java.nio.charset.StandardCharsets._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.test.SharedSQLContext
  //兼容的文件流日志套件
class CompactibleFileStreamLogSuite extends SparkFunSuite with SharedSQLContext {

  /** To avoid caching of FS objects
    * 避免FS对象的高速缓存*/
  override protected def sparkConf =
    super.sparkConf.set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")

  import CompactibleFileStreamLog._

  /** -- testing of `object CompactibleFileStreamLog` begins -- */
  //从文件名获取批次ID
  test("getBatchIdFromFileName") {
    assert(1234L === getBatchIdFromFileName("1234"))
    assert(1234L === getBatchIdFromFileName("1234.compact"))
    intercept[NumberFormatException] {
      getBatchIdFromFileName("1234a")
    }
  }
  //是压实批
  test("isCompactionBatch") {
    assert(false === isCompactionBatch(0, compactInterval = 3))
    assert(false === isCompactionBatch(1, compactInterval = 3))
    assert(true === isCompactionBatch(2, compactInterval = 3))
    assert(false === isCompactionBatch(3, compactInterval = 3))
    assert(false === isCompactionBatch(4, compactInterval = 3))
    assert(true === isCompactionBatch(5, compactInterval = 3))
  }
  //下一个压实批次ID
  test("nextCompactionBatchId") {
    assert(2 === nextCompactionBatchId(0, compactInterval = 3))
    assert(2 === nextCompactionBatchId(1, compactInterval = 3))
    assert(5 === nextCompactionBatchId(2, compactInterval = 3))
    assert(5 === nextCompactionBatchId(3, compactInterval = 3))
    assert(5 === nextCompactionBatchId(4, compactInterval = 3))
    assert(8 === nextCompactionBatchId(5, compactInterval = 3))
  }
  //在压实批之前获取有效批次
  test("getValidBatchesBeforeCompactionBatch") {
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(0, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(1, compactInterval = 3)
    }
    assert(Seq(0, 1) === getValidBatchesBeforeCompactionBatch(2, compactInterval = 3))
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(3, compactInterval = 3)
    }
    intercept[AssertionError] {
      getValidBatchesBeforeCompactionBatch(4, compactInterval = 3)
    }
    assert(Seq(2, 3, 4) === getValidBatchesBeforeCompactionBatch(5, compactInterval = 3))
  }
  //获得所有有效批次
  test("getAllValidBatches") {
    assert(Seq(0) === getAllValidBatches(0, compactInterval = 3))
    assert(Seq(0, 1) === getAllValidBatches(1, compactInterval = 3))
    assert(Seq(2) === getAllValidBatches(2, compactInterval = 3))
    assert(Seq(2, 3) === getAllValidBatches(3, compactInterval = 3))
    assert(Seq(2, 3, 4) === getAllValidBatches(4, compactInterval = 3))
    assert(Seq(5) === getAllValidBatches(5, compactInterval = 3))
    assert(Seq(5, 6) === getAllValidBatches(6, compactInterval = 3))
    assert(Seq(5, 6, 7) === getAllValidBatches(7, compactInterval = 3))
    assert(Seq(8) === getAllValidBatches(8, compactInterval = 3))
  }
  //派生紧凑间隔
  test("deriveCompactInterval") {
    // latestCompactBatchId(4) + 1 <= default(5)
    // then use latestestCompactBatchId + 1 === 5
    assert(5 === deriveCompactInterval(5, 4))
    // First divisor of 10 greater than 4 === 5
    assert(5 === deriveCompactInterval(4, 9))
  }

  /** -- testing of `object CompactibleFileStreamLog` ends -- */
  //批次标识为路径
  test("batchIdToPath") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      defaultCompactInterval = 3,
      defaultMinBatchesToRetain = 1,
      compactibleLog => {
        assert("0" === compactibleLog.batchIdToPath(0).getName)
        assert("1" === compactibleLog.batchIdToPath(1).getName)
        assert("2.compact" === compactibleLog.batchIdToPath(2).getName)
        assert("3" === compactibleLog.batchIdToPath(3).getName)
        assert("4" === compactibleLog.batchIdToPath(4).getName)
        assert("5.compact" === compactibleLog.batchIdToPath(5).getName)
      })
  }
  //序列化
  test("serialize") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      defaultCompactInterval = 3,
      defaultMinBatchesToRetain = 1,
      compactibleLog => {
        val logs = Array("entry_1", "entry_2", "entry_3")
        val expected = s"""v${FakeCompactibleFileStreamLog.VERSION}
            |"entry_1"
            |"entry_2"
            |"entry_3"""".stripMargin
        val baos = new ByteArrayOutputStream()
        compactibleLog.serialize(logs, baos)
        assert(expected === baos.toString(UTF_8.name()))

        baos.reset()
        compactibleLog.serialize(Array(), baos)
        assert(s"v${FakeCompactibleFileStreamLog.VERSION}" === baos.toString(UTF_8.name()))
      })
  }
  //反序列化
  test("deserialize") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      defaultCompactInterval = 3,
      defaultMinBatchesToRetain = 1,
      compactibleLog => {
        val logs = s"""v${FakeCompactibleFileStreamLog.VERSION}
            |"entry_1"
            |"entry_2"
            |"entry_3"""".stripMargin
        val expected = Array("entry_1", "entry_2", "entry_3")
        assert(expected ===
          compactibleLog.deserialize(new ByteArrayInputStream(logs.getBytes(UTF_8))))

        assert(Nil ===
          compactibleLog.deserialize(
            new ByteArrayInputStream(s"v${FakeCompactibleFileStreamLog.VERSION}".getBytes(UTF_8))))
      })
  }
  //由未来版本编写的反序列化日志
  test("deserialization log written by future version") {
    withTempDir { dir =>
      def newFakeCompactibleFileStreamLog(version: Int): FakeCompactibleFileStreamLog =
        new FakeCompactibleFileStreamLog(
          version,
          //这个参数在这个测试用例中并不重要
          _fileCleanupDelayMs = Long.MaxValue, // this param does not matter here in this test case
          //这个参数在这个测试用例中并不重要
          _defaultCompactInterval = 3,         // this param does not matter here in this test case
          //这个参数在这个测试用例中并不重要
          _defaultMinBatchesToRetain = 1,      // this param does not matter here in this test case
          spark,
          dir.getCanonicalPath)

      val writer = newFakeCompactibleFileStreamLog(version = 2)
      val reader = newFakeCompactibleFileStreamLog(version = 1)
      writer.add(0, Array("entry"))
      val e = intercept[IllegalStateException] {
        reader.get(0)
      }
      Seq(
        "maximum supported log version is v1, but encountered v2",
        "produced by a newer version of Spark and cannot be read by this version"
      ).foreach { message =>
        assert(e.getMessage.contains(message))
      }
    }
  }
  //紧凑
  test("compact") {
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = Long.MaxValue,
      defaultCompactInterval = 3,
      defaultMinBatchesToRetain = 1,
      compactibleLog => {
        for (batchId <- 0 to 10) {
          compactibleLog.add(batchId, Array("some_path_" + batchId))
          val expectedFiles = (0 to batchId).map { id => "some_path_" + id }
          assert(compactibleLog.allFiles() === expectedFiles)
          if (isCompactionBatch(batchId, 3)) {
            // Since batchId is a compaction batch, the batch log file should contain all logs
            //由于batchId是一个压缩批处理，批处理日志文件应包含所有日志
            assert(compactibleLog.get(batchId).getOrElse(Nil) === expectedFiles)
          }
        }
      })
  }
  //删除过期的文件
  test("delete expired file") {
    // Set `fileCleanupDelayMs` to 0 so that we can detect the deleting behaviour deterministically
    //将`fileCleanupDelayMs`设置为0，以便我们可以确定性地检测删除行为
    withFakeCompactibleFileStreamLog(
      fileCleanupDelayMs = 0,
      defaultCompactInterval = 3,
      defaultMinBatchesToRetain = 1,
      compactibleLog => {
        val fs = compactibleLog.metadataPath.getFileSystem(spark.sessionState.newHadoopConf())

        def listBatchFiles(): Set[String] = {
          fs.listStatus(compactibleLog.metadataPath).map(_.getPath.getName).filter { fileName =>
            try {
              getBatchIdFromFileName(fileName)
              true
            } catch {
              case _: NumberFormatException => false
            }
          }.toSet
        }

        compactibleLog.add(0, Array("some_path_0"))
        assert(Set("0") === listBatchFiles())
        compactibleLog.add(1, Array("some_path_1"))
        assert(Set("0", "1") === listBatchFiles())
        compactibleLog.add(2, Array("some_path_2"))
        assert(Set("0", "1", "2.compact") === listBatchFiles())
        compactibleLog.add(3, Array("some_path_3"))
        assert(Set("2.compact", "3") === listBatchFiles())
        compactibleLog.add(4, Array("some_path_4"))
        assert(Set("2.compact", "3", "4") === listBatchFiles())
        compactibleLog.add(5, Array("some_path_5"))
        assert(Set("2.compact", "3", "4", "5.compact") === listBatchFiles())
        compactibleLog.add(6, Array("some_path_6"))
        assert(Set("5.compact", "6") === listBatchFiles())
      })
  }

  private def withFakeCompactibleFileStreamLog(
    fileCleanupDelayMs: Long,
    defaultCompactInterval: Int,
    defaultMinBatchesToRetain: Int,
    f: FakeCompactibleFileStreamLog => Unit
  ): Unit = {
    withTempDir { file =>
      val compactibleLog = new FakeCompactibleFileStreamLog(
        FakeCompactibleFileStreamLog.VERSION,
        fileCleanupDelayMs,
        defaultCompactInterval,
        defaultMinBatchesToRetain,
        spark,
        file.getCanonicalPath)
      f(compactibleLog)
    }
  }
}

object FakeCompactibleFileStreamLog {
  val VERSION = 1
}

class FakeCompactibleFileStreamLog(
    metadataLogVersion: Int,
    _fileCleanupDelayMs: Long,
    _defaultCompactInterval: Int,
    _defaultMinBatchesToRetain: Int,
    sparkSession: SparkSession,
    path: String)
  extends CompactibleFileStreamLog[String](
    metadataLogVersion,
    sparkSession,
    path
  ) {

  override protected def fileCleanupDelayMs: Long = _fileCleanupDelayMs

  override protected def isDeletingExpiredLog: Boolean = true

  override protected def defaultCompactInterval: Int = _defaultCompactInterval

  override protected val minBatchesToRetain: Int = _defaultMinBatchesToRetain

  override def compactLogs(logs: Seq[String]): Seq[String] = logs
}
