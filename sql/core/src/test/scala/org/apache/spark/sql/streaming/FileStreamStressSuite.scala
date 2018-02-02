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

import java.io.File
import java.util.UUID

import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.util._
import org.apache.spark.util.Utils

/**
 * A stress test for streaming queries that read and write files.  This test consists of
 * two threads:
  * 对读取和写入文件的流式查询进行压力测试,这个测试包含两个线程：
  *
 *  - one that writes out `numRecords` distinct integers to files of random sizes (the total
 *    number of records is fixed but each files size / creation time is random).
  *    一个写出`numRecords`不同大小的随机大小的文件（记录总数是固定的，但每个文件大小/创建时间是随机的）
 *  - another that continually restarts a buggy streaming query (i.e. fails with 5% probability on
 *    any partition).
 *    另一个不断重新启动错误的流式查询（即在任何分区上失败的概率为5％）
 * At the end, the resulting files are loaded and the answer is checked.
  * 最后,加载结果文件并检查答案
 */
class FileStreamStressSuite extends StreamTest {
  import testImplicits._

  // Error message thrown in the streaming job for testing recovery.
  //在测试恢复的流式作业中引发错误消息
  private val injectedErrorMsg = "test suite injected failure!"
  //容错应力测试 - 未分区的输出
  testQuietly("fault tolerance stress test - unpartitioned output") {
    stressTest(partitionWrites = false)
  }
  //容错应力测试 - 分区输出
  testQuietly("fault tolerance stress test - partitioned output") {
    stressTest(partitionWrites = true)
  }

  def stressTest(partitionWrites: Boolean): Unit = {
    val numRecords = 10000
    val inputDir = Utils.createTempDir(namePrefix = "stream.input").getCanonicalPath
    val stagingDir = Utils.createTempDir(namePrefix = "stream.staging").getCanonicalPath
    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpoint = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    @volatile
    var continue = true
    @volatile
    var stream: StreamingQuery = null

    val writer = new Thread("stream writer") {
      override def run(): Unit = {
        var i = numRecords
        while (i > 0) {
          val count = Random.nextInt(100)
          var j = 0
          var string = ""
          while (j < count && i > 0) {
            if (i % 10000 == 0) { logError(s"Wrote record $i") }
            string = string + i + "\n"
            j += 1
            i -= 1
          }

          val uuid = UUID.randomUUID().toString
          val fileName = new File(stagingDir, uuid)
          stringToFile(fileName, string)
          fileName.renameTo(new File(inputDir, uuid))
          val sleep = Random.nextInt(100)
          Thread.sleep(sleep)
        }

        logError("== DONE WRITING ==")
        var done = false
        while (!done) {
          try {
            stream.processAllAvailable()
            done = true
          } catch {
            case NonFatal(_) =>
          }
        }

        continue = false
        stream.stop()
      }
    }
    writer.start()
    //文件流读取方式DataFrame
    val input = spark.readStream.format("text").load(inputDir)

    def startStream(): StreamingQuery = {
      //解决序列化问题
      val errorMsg = injectedErrorMsg  // work around serialization issue
      val output = input
        .repartition(5)
        .as[String]
        .mapPartitions { iter =>
          val rand = Random.nextInt(100)
          if (rand < 10) {
            sys.error(errorMsg)
          }
          iter.map(_.toLong)
        }
        .map(x => (x % 400, x.toString))
        .toDF("id", "data")

      if (partitionWrites) {
        output
          .writeStream
          .partitionBy("id")
          //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          .format("parquet")
          .option("checkpointLocation", checkpoint)
          .start(outputDir)
      } else {
        output
          .writeStream
          //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          .format("parquet")
          .option("checkpointLocation", checkpoint)
          .start(outputDir)
      }
    }

    var failures = 0
    while (continue) {
      if (failures % 10 == 0) { logError(s"Query restart #$failures") }
      stream = startStream()

      try {
        stream.awaitTermination()
      } catch {
        case e: StreamingQueryException
          if e.getCause != null && e.getCause.getCause != null &&
              e.getCause.getCause.getMessage.contains(injectedErrorMsg) =>
          // Getting the expected error message
          //获取预期的错误消息
          failures += 1
      }
    }

    logError(s"Stream restarted $failures times.")
    assert(spark.read.parquet(outputDir).distinct().count() == numRecords)
  }
}
