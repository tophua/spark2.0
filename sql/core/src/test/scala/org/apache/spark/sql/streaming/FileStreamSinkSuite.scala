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

import java.util.Locale

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

class FileStreamSinkSuite extends StreamTest {
  import testImplicits._
  //未分区的文字和批量阅读
  test("unpartitioned writing and batch reading") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      query =
        df.writeStream
          .option("checkpointLocation", checkpointDir)
          //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          .format("parquet")
          .start(outputDir)

      inputData.addData(1, 2, 3)

      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }

      val outputDf = spark.read.parquet(outputDir).as[Int]
      checkDatasetUnorderly(outputDf, 1, 2, 3)

    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }
  //正确编码和解码路径
  test("SPARK-21167: encode and decode path correctly") {
    val inputData = MemoryStream[String]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    val query = ds.map(s => (s, s.length))
      .toDF("value", "len")
      .writeStream
      .partitionBy("value")
      .option("checkpointLocation", checkpointDir)
      //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
      .format("parquet")
      .start(outputDir)

    try {
      // The output is partitoned by "value", so the value will appear in the file path.
      // This is to test if we handle spaces in the path correctly.
      //输出由“值”分区,因此该值将出现在文件路径中
      //这是为了测试我们是否正确处理路径中的空格
      inputData.addData("hello world")
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }
      val outputDf = spark.read.parquet(outputDir)
      checkDatasetUnorderly(outputDf.as[(Int, String)], ("hello world".length, "hello world"))
    } finally {
      query.stop()
    }
  }
  //分区写作和批量阅读
  test("partitioned writing and batch reading") {
    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir)
          //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          .format("parquet")
          .start(outputDir)

      inputData.addData(1, 2, 3)
      failAfter(streamingTimeout) {
        query.processAllAvailable()
      }

      val outputDf = spark.read.parquet(outputDir)
      val expectedSchema = new StructType()
        .add(StructField("value", IntegerType, nullable = false))
        .add(StructField("id", IntegerType))
      assert(outputDf.schema === expectedSchema)

      // Verify that MetadataLogFileIndex is being used and the correct partitioning schema has
      // been inferred
      //验证是否正在使用MetadataLogFileIndex,并且推断了正确的分区模式
      val hadoopdFsRelations = outputDf.queryExecution.analyzed.collect {
        case LogicalRelation(baseRelation: HadoopFsRelation, _, _, _) => baseRelation
      }
      assert(hadoopdFsRelations.size === 1)
      assert(hadoopdFsRelations.head.location.isInstanceOf[MetadataLogFileIndex])
      assert(hadoopdFsRelations.head.partitionSchema.exists(_.name == "id"))
      assert(hadoopdFsRelations.head.dataSchema.exists(_.name == "value"))

      // Verify the data is correctly read
      //验证数据是否正确读取
      checkDatasetUnorderly(
        outputDf.as[(Int, Int)],
        (1000, 1), (2000, 2), (3000, 3))

      /**
        * Check some condition on the partitions of the FileScanRDD generated by a DF
        * 检查由DF生成的FileScanRDD分区上的一些条件
        * */
      def checkFileScanPartitions(df: DataFrame)(func: Seq[FilePartition] => Unit): Unit = {
        val getFileScanRDD = df.queryExecution.executedPlan.collect {
          case scan: DataSourceScanExec if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
            scan.inputRDDs().head.asInstanceOf[FileScanRDD]
        }.headOption.getOrElse {
          fail(s"No FileScan in query\n${df.queryExecution}")
        }
        func(getFileScanRDD.filePartitions)
      }

      // Read without pruning
      checkFileScanPartitions(outputDf) { partitions =>
        // There should be as many distinct partition values as there are distinct ids
        //应该有许多不同的分区值,因为有不同的ID
        assert(partitions.flatMap(_.files.map(_.partitionValues)).distinct.size === 3)
      }

      // Read with pruning, should read only files in partition dir id=1
      //读取修剪,应该只读取分区dir id = 1中的文件
      checkFileScanPartitions(outputDf.filter("id = 1")) { partitions =>
        val filesToBeRead = partitions.flatMap(_.files)
        assert(filesToBeRead.map(_.filePath).forall(_.contains("/id=1/")))
        assert(filesToBeRead.map(_.partitionValues).distinct.size === 1)
      }

      // Read with pruning, should read only files in partition dir id=1 and id=2
      //阅读与修剪,应该只读取分区dir id = 1和id = 2中的文件
      checkFileScanPartitions(outputDf.filter("id in (1,2)")) { partitions =>
        val filesToBeRead = partitions.flatMap(_.files)
        assert(!filesToBeRead.map(_.filePath).exists(_.contains("/id=3/")))
        assert(filesToBeRead.map(_.partitionValues).distinct.size === 2)
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }
  //分区写入和批量读取'basePath'
  test("partitioned writing and batch reading with 'basePath'") {
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        val outputPath = outputDir.getAbsolutePath
        val inputData = MemoryStream[Int]
        val ds = inputData.toDS()

        var query: StreamingQuery = null

        try {
          query =
            ds.map(i => (i, -i, i * 1000))
              .toDF("id1", "id2", "value")
              .writeStream
              .partitionBy("id1", "id2")
              .option("checkpointLocation", checkpointDir.getAbsolutePath)
              //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
              .format("parquet")
              .start(outputPath)

          inputData.addData(1, 2, 3)
          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }

          val readIn = spark.read.option("basePath", outputPath).parquet(s"$outputDir/*/*")
          checkDatasetUnorderly(
            readIn.as[(Int, Int, Int)],
            (1000, 1, -1), (2000, 2, -2), (3000, 3, -3))
        } finally {
          if (query != null) {
            query.stop()
          }
        }
      }
    }
  }

  // This tests whether FileStreamSink works with aggregations. Specifically, it tests
  // whether the correct streaming QueryExecution (i.e. IncrementalExecution) is used to
  // to execute the trigger for writing data to file sink. See SPARK-18440 for more details.
  //用聚合书写
  test("writing with aggregation") {

    // Since FileStreamSink currently only supports append mode, we will test FileStreamSink
    // with aggregations using event time windows and watermark, which allows
    // aggregation + append mode.
    //由于FileStreamSink目前仅支持追加模式，因此我们将使用事件时间窗口和水印来聚合测试FileStreamSink，这允许聚合+追加模式。
    val inputData = MemoryStream[Long]
    val inputDF = inputData.toDF.toDF("time")
    val outputDf = inputDF
      .selectExpr("CAST(time AS timestamp) AS timestamp")
      /**
        * 在Spark 2.1中，我们引入了水印，让我们的引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧的状态。
        * 您可以通过指定事件时间列和根据事件时间预计数据延迟的阈值来定义查询的水印。对于在时间T开始的特定窗口，
        * 引擎将保持状态并允许后期数据更新状态，直到（由引擎看到的最大事件时间 - 后期阈值> T）。
        * 换句话说，阈值内的晚数据将被聚合，但晚于阈值的数据将被丢弃。
        * 我们定义查询的水印对列“timestamp”的值，并且还定义“10分钟”作为允许数据超时的阈值。
        * 如果此查询在Append输出模式（稍后在“输出模式”部分中讨论）中运行，则引擎将从列“timestamp”跟踪当前事件时间，
        * 并在最终确定窗口计数和添加之前等待事件时间的额外“10分钟”他们到结果表
        */
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "5 seconds"))
      .count()
      .select("window.start", "window.end", "count")

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      query =
        outputDf.writeStream
          .option("checkpointLocation", checkpointDir)
          //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          .format("parquet")
          .start(outputDir)


      def addTimestamp(timestampInSecs: Int*): Unit = {
        inputData.addData(timestampInSecs.map(_ * 1L): _*)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }
      }

      def check(expectedResult: ((Long, Long), Long)*): Unit = {
        val outputDf = spark.read.parquet(outputDir)
          .selectExpr(
            "CAST(start as BIGINT) AS start",
            "CAST(end as BIGINT) AS end",
            "count")
        checkDataset(
          outputDf.as[(Long, Long, Long)],
          expectedResult.map(x => (x._1._1, x._1._2, x._2)): _*)
      }

      addTimestamp(100) // watermark = None before this, watermark = 100 - 10 = 90 after this
      check() // nothing emitted yet

      addTimestamp(104, 123) // watermark = 90 before this, watermark = 123 - 10 = 113 after this
      check() // nothing emitted yet

      addTimestamp(140) // wm = 113 before this, emit results on 100-105, wm = 130 after this
      check((100L, 105L) -> 2L)

      addTimestamp(150) // wm = 130s before this, emit results on 120-125, wm = 150 after this
      check((100L, 105L) -> 2L, (120L, 125L) -> 1L)

    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }
  //不支持更新和完成输出模式
  //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
  test("Update and Complete output mode not supported") {
    val df = MemoryStream[Int].toDF().groupBy().count()
    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath

    withTempDir { dir =>

      def testOutputMode(mode: String): Unit = {
        val e = intercept[AnalysisException] {
          //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          df.writeStream.format("parquet").outputMode(mode).start(dir.getCanonicalPath)
        }
        Seq(mode, "not support").foreach { w =>
          assert(e.getMessage.toLowerCase(Locale.ROOT).contains(w))
        }
      }

      testOutputMode("update")
      //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
      testOutputMode("complete")
    }
  }

  test("parquet") {
    //不指定时,不应该将错误作为默认格式parquet抛出
    testFormat(None) // should not throw error as default format parquet when not specified
    testFormat(Some("parquet"))
  }

  test("text") {
    testFormat(Some("text"))
  }

  test("json") {
    testFormat(Some("json"))
  }

  def testFormat(format: Option[String]): Unit = {
    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    var query: StreamingQuery = null

    try {
      val writer = ds.map(i => (i, i * 1000)).toDF("id", "value").writeStream
      if (format.nonEmpty) {
        writer.format(format.get)
      }
      query = writer.option("checkpointLocation", checkpointDir).start(outputDir)
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("FileStreamSink.ancestorIsMetadataDirectory()") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    def assertAncestorIsMetadataDirectory(path: String): Unit =
      assert(FileStreamSink.ancestorIsMetadataDirectory(new Path(path), hadoopConf))
    def assertAncestorIsNotMetadataDirectory(path: String): Unit =
      assert(!FileStreamSink.ancestorIsMetadataDirectory(new Path(path), hadoopConf))

    assertAncestorIsMetadataDirectory(s"/${FileStreamSink.metadataDir}")
    assertAncestorIsMetadataDirectory(s"/${FileStreamSink.metadataDir}/")
    assertAncestorIsMetadataDirectory(s"/a/${FileStreamSink.metadataDir}")
    assertAncestorIsMetadataDirectory(s"/a/${FileStreamSink.metadataDir}/")
    assertAncestorIsMetadataDirectory(s"/a/b/${FileStreamSink.metadataDir}/c")
    assertAncestorIsMetadataDirectory(s"/a/b/${FileStreamSink.metadataDir}/c/")

    assertAncestorIsNotMetadataDirectory(s"/a/b/c")
    assertAncestorIsNotMetadataDirectory(s"/a/b/c/${FileStreamSink.metadataDir}extra")
  }
  //检查模式中的名称重复
  test("SPARK-20460 Check name duplication in schema") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val inputData = MemoryStream[(Int, Int)]
        val df = inputData.toDF()

        val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
        val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

        var query: StreamingQuery = null
        try {
          query =
            df.writeStream
              .option("checkpointLocation", checkpointDir)
              .format("json")
              .start(outputDir)

          inputData.addData((1, 1))

          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }
        } finally {
          if (query != null) {
            query.stop()
          }
        }

        val errorMsg = intercept[AnalysisException] {
          spark.read.schema(s"$c0 INT, $c1 INT").json(outputDir).as[(Int, Int)]
        }.getMessage
        assert(errorMsg.contains("Found duplicate column(s) in the data schema: "))
      }
    }
  }
}
