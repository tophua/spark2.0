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

package org.apache.spark.sql.streaming.test

import java.io.File
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.apache.hadoop.fs.Path
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.{ProcessingTime => DeprecatedProcessingTime, _}
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

object LastOptions {

  var mockStreamSourceProvider = mock(classOf[StreamSourceProvider])
  var mockStreamSinkProvider = mock(classOf[StreamSinkProvider])
  //Map对象初始赋值为null
  var parameters: Map[String, String] = null
  //Option对象初始赋值null
  var schema: Option[StructType] = null
    //Seq初始赋值Nil
  var partitionColumns: Seq[String] = Nil
  //定义函数,赋值默认值
  def clear(): Unit = {
    parameters = null
    schema = null
    partitionColumns = null
    reset(mockStreamSourceProvider)
    reset(mockStreamSinkProvider)
  }
}

/**
  * Dummy provider: returns no-op source/sink and records options in [[LastOptions]].
  * 虚拟提供者：返回无操作源/接收器并在[[LastOptions]]中记录选项
  * */
class DefaultSource extends StreamSourceProvider with StreamSinkProvider {

  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    LastOptions.parameters = parameters
    LastOptions.schema = schema
    LastOptions.mockStreamSourceProvider.sourceSchema(spark, schema, providerName, parameters)
    ("dummySource", fakeSchema)
  }

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    LastOptions.parameters = parameters
    LastOptions.schema = schema
    LastOptions.mockStreamSourceProvider.createSource(
      spark, metadataPath, schema, providerName, parameters)
    new Source {
      override def schema: StructType = fakeSchema

      override def getOffset: Option[Offset] = Some(new LongOffset(0))

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        import spark.implicits._
        //创建DataFrame
        spark.internalCreateDataFrame(spark.sparkContext.emptyRDD, schema, isStreaming = true)
      }

      override def stop() {}
    }
  }
//"Output"是写入到外部存储的写方式
  override def createSink(
      spark: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    LastOptions.parameters = parameters
    LastOptions.partitionColumns = partitionColumns
    LastOptions.mockStreamSinkProvider.createSink(spark, parameters, partitionColumns, outputMode)
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = {}
    }
  }
}

class DataStreamReaderWriterSuite extends StreamTest with BeforeAndAfter {

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  after {
    // 输出接收器 Foreach sink - 对输出中的记录运行任意计算。
    spark.streams.active.foreach(_.stop())
  }
  //不能在流式数据集上调用写入
  test("write cannot be called on streaming datasets") {
    val e = intercept[AnalysisException] {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load()
        .write
        .save()
    }
    Seq("'write'", "not", "streaming Dataset/DataFrame").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }
  //解决默认的来源
  test("resolve default source") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()
      .writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()
  }
  //解决全类
  test("resolve full class") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test.DefaultSource")
      .load()
      .writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()
  }

  test("options") {
    val map = new java.util.HashMap[String, String]
    map.put("opt3", "3")

    val df = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .option("opt1", "1")
        .options(Map("opt2" -> "2"))
        .options(map)
        .load()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")

    LastOptions.clear()

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("opt1", "1")
      .options(Map("opt2" -> "2"))
      .options(map)
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()

    assert(LastOptions.parameters("opt1") == "1")
    assert(LastOptions.parameters("opt2") == "2")
    assert(LastOptions.parameters("opt3") == "3")
  }
//分区
  test("partitioning") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start()
      .stop()
    assert(LastOptions.partitionColumns == Nil)

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .partitionBy("a")
      .start()
      .stop()
    assert(LastOptions.partitionColumns == Seq("a"))

    withSQLConf("spark.sql.caseSensitive" -> "false") {
      df.writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .partitionBy("A")
        .start()
        .stop()
      assert(LastOptions.partitionColumns == Seq("a"))
    }

    intercept[AnalysisException] {
      df.writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .partitionBy("b")
        .start()
        .stop()
    }
  }
  //流路径
  test("stream paths") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .load("/test")

    assert(LastOptions.parameters("path") == "/test")

    LastOptions.clear()

    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .start("/test")
      .stop()

    assert(LastOptions.parameters("path") == "/test")
  }
  //测试不同的数据类型的选项
  test("test different data types for options") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .load("/test")

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")

    LastOptions.clear()
    df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("intOpt", 56)
      .option("boolOpt", false)
      .option("doubleOpt", 6.7)
      .option("checkpointLocation", newMetadataDir)
      .start("/test")
      .stop()

    assert(LastOptions.parameters("intOpt") == "56")
    assert(LastOptions.parameters("boolOpt") == "false")
    assert(LastOptions.parameters("doubleOpt") == "6.7")
  }
  //唯一的查询名称
  test("unique query names") {

    /**
      * Start a query with a specific name
      * 用一个特定的名字开始查询,StreamingQuery对象，它是连续运行的执行的句柄
      * */
    def startQueryWithName(name: String = ""): StreamingQuery = {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load("/test")
        .writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        .queryName(name)
        //start（）才能真正开始执行查询,这返回一个StreamingQuery对象,它是连续运行的执行的句柄
        .start()
    }

    /**
      * Start a query without specifying a name
      * 开始查询而不指定名称
      * */
    def startQueryWithoutName(): StreamingQuery = {
      spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load("/test")
        .writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", newMetadataDir)
        //start（）才能真正开始执行查询,这返回一个StreamingQuery对象,它是连续运行的执行的句柄
        .start()
    }

    /**
      * Get the names of active streams
      * 获取活动流的名称
      * */
    def activeStreamNames: Set[String] = {
      val streams = spark.streams.active
      val names = streams.map(_.name).toSet
      assert(streams.length === names.size, s"names of active queries are not unique: $names")
      names
    }

    val q1 = startQueryWithName("name")

    // Should not be able to start another query with the same name
    //应该无法启动具有相同名称的另一个查询
    intercept[IllegalArgumentException] {
      startQueryWithName("name")
    }
    assert(activeStreamNames === Set("name"))

    // Should be able to start queries with other names
    //应该能够用其他名字开始查询
    val q3 = startQueryWithName("another-name")
    assert(activeStreamNames === Set("name", "another-name"))

    // Should be able to start queries with auto-generated names
    //应该能够使用自动生成的名称开始查询
    val q4 = startQueryWithoutName()
    assert(activeStreamNames.contains(q4.name))

    // Should not be able to start a query with same auto-generated name
    //不应该能够使用相同的自动生成的名称启动查询
    intercept[IllegalArgumentException] {
      startQueryWithName(q4.name)
    }

    // Should be able to start query with that name after stopping the previous query
    //在停止以前的查询之后,应该能够使用该名称开始查询
    q1.stop()
    val q5 = startQueryWithName("name")
    assert(activeStreamNames.contains("name"))
    // 输出接收器 Foreach sink - 对输出中的记录运行任意计算。
    spark.streams.active.foreach(_.stop())
  }

  test("trigger") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load("/test")

    var q = df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.stop()

    assert(q.asInstanceOf[StreamingQueryWrapper].streamingQuery.trigger == ProcessingTime(10000))

    q = df.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(100, TimeUnit.SECONDS))
      .start()
    q.stop()

    assert(q.asInstanceOf[StreamingQueryWrapper].streamingQuery.trigger == ProcessingTime(100000))
  }
  //源元数据路径
  test("source metadataPath") {
    LastOptions.clear()

    val checkpointLocationURI = new Path(newMetadataDir).toUri

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocationURI.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    verify(LastOptions.mockStreamSourceProvider).createSource(
      any(),
      meq(s"${makeQualifiedPath(checkpointLocationURI.toString)}/sources/0"),
      meq(None),
      meq("org.apache.spark.sql.streaming.test"),
      meq(Map.empty))

    verify(LastOptions.mockStreamSourceProvider).createSource(
      any(),
      meq(s"${makeQualifiedPath(checkpointLocationURI.toString)}/sources/1"),
      meq(None),
      meq("org.apache.spark.sql.streaming.test"),
      meq(Map.empty))
  }

  private def newTextInput = Utils.createTempDir(namePrefix = "text").getCanonicalPath
  //检查foreach()捕获空写
  test("check foreach() catches null writers") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    var w = df.writeStream
    var e = intercept[IllegalArgumentException](w.foreach(null))
    Seq("foreach", "null").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }

  //检查foreach()不支持分区
  test("check foreach() does not support partitioning") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()
    val foreachWriter = new ForeachWriter[Row] {
      override def open(partitionId: Long, version: Long): Boolean = false
      override def process(value: Row): Unit = {}
      override def close(errorOrNull: Throwable): Unit = {}
    }
    var w = df.writeStream.partitionBy("value")
    var e = intercept[AnalysisException](w.foreach(foreachWriter).start())
    Seq("foreach", "partitioning").foreach { s =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)))
    }
  }
  //ConsoleSink可以正确加载
  test("ConsoleSink can be correctly loaded") {
    LastOptions.clear()
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()

    val sq = df.writeStream
      //控制台接收器（用于调试） - 每次有触发器时将输出打印到控制台/ stdout。
      // 这应该用于低数据量上的调试目的,因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
      .format("console")
      .option("checkpointLocation", newMetadataDir)
      .trigger(ProcessingTime(2.seconds))
      .start()

    sq.awaitTermination(2000L)
  }
//防止所有列分区
  test("prevent all column partitioning") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      intercept[AnalysisException] {
        spark.range(10).writeStream
        //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
          //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
          //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
          .outputMode("append")
          .partitionBy("id")
          //文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
          .format("parquet")
          .start(path)
      }
    }
  }
//ConsoleSink不应该要求checkpointLocation
  test("ConsoleSink should not require checkpointLocation") {
    LastOptions.clear()
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .load()
    //控制台接收器（用于调试） - 每次有触发器时将输出打印到控制台/ stdout。
    // 这应该用于低数据量上的调试目的,因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
    val sq = df.writeStream.format("console").start()
    sq.stop()
  }

  private def testMemorySinkCheckpointRecovery(chkLoc: String, provideInWriter: Boolean): Unit = {
    import testImplicits._
    val ms = new MemoryStream[Int](0, sqlContext)
    val df = ms.toDF().toDF("a")
    val tableName = "test"
    def startQuery: StreamingQuery = {
      val writer = df.groupBy("a")
        .count()
        .writeStream
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        .queryName(tableName)
          //每次触发后，整个结果表将输出到接收器。聚合查询支持此选项。
        //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
        .outputMode("complete")
      if (provideInWriter) {
        writer.option("checkpointLocation", chkLoc)
      }
      writer.start()
    }
    // no exception here
    val q = startQuery
    ms.addData(0, 1)
    q.processAllAvailable()
    q.stop()

    checkAnswer(
      spark.table(tableName),
      Seq(Row(0, 1), Row(1, 1))
    )
    spark.sql(s"drop table $tableName")
    // verify table is dropped
    //验证表被丢弃
    intercept[AnalysisException](spark.table(tableName).collect())
    val q2 = startQuery
    ms.addData(0)
    q2.processAllAvailable()
    checkAnswer(
      spark.table(tableName),
      Seq(Row(0, 2), Row(1, 1))
    )

    q2.stop()
  }
  //MemorySink可以在完整模式下从检查点恢复
  //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
  //每次触发后，整个结果表将输出到接收器。聚合查询支持此选项。
  test("MemorySink can recover from a checkpoint in Complete Mode") {
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "offsets")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())
    testMemorySinkCheckpointRecovery(checkpointLoc, provideInWriter = true)
  }
//MemorySink可以从完整模式下conf中提供的检查点恢复
  //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
  test("SPARK-18927: MemorySink can recover from a checkpoint provided in conf in Complete Mode") {
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "offsets")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())
    withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointLoc) {
      testMemorySinkCheckpointRecovery(checkpointLoc, provideInWriter = false)
    }
  }
  //追加模式内存接收器不支持检查点恢复
  //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
  //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
  //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
  test("append mode memory sink's do not support checkpoint recovery") {
    import testImplicits._
    val ms = new MemoryStream[Int](0, sqlContext)
    val df = ms.toDF().toDF("a")
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "offsets")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())

    val e = intercept[AnalysisException] {
      df.writeStream
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        .queryName("test")
        .option("checkpointLocation", checkpointLoc)
        //其中只有自上次触发后添加到结果表中的新行将输出到接收器。这仅支持那些添加到结果表中的行从不会更改的查询。
        //因此,该模式保证每行只输出一次（假设容错宿）。例如，只有select，where，map，flatMap，filter，join等的查询将支持Append模式。
        //Append模式：只有自上次触发后在结果表中附加的新行将被写入外部存储器。这仅适用于结果表中的现有行不会更改的查询。
        .outputMode("append")
        .start()
    }
    assert(e.getMessage.contains("does not support recovering"))
    assert(e.getMessage.contains("checkpoint location"))
  }
//使用用户指定的类型为文件源中的分区列
  test("SPARK-18510: use user specified types for partition columns in file sources") {
    import org.apache.spark.sql.functions.udf
    import testImplicits._
    withTempDir { src =>
      val createArray = udf { (length: Long) =>
        for (i <- 1 to length.toInt) yield i.toString
      }
      spark.range(4).select(createArray('id + 1) as 'ex, 'id, 'id % 4 as 'part).coalesce(1).write
        .partitionBy("part", "id")
        .mode("overwrite")
        .parquet(src.toString)
      // Specify a random ordering of the schema, partition column in the middle, etc.
      //指定模式的随机排序,中间的分区列等
      // Also let's say that the partition columns are Strings instead of Longs.
      //还有,我们可以说分区列是字符串而不是长整型
      // partition columns should go to the end
      //分区列应该走到最后
      val schema = new StructType()
        .add("id", StringType)
        .add("ex", ArrayType(StringType))

      val sdf = spark.readStream
        .schema(schema)
        //输出接收器 文件接收器 - 将输出存储到目录,支持对分区表的写入。按时间分区可能有用。
        .format("parquet")
        .load(src.toString)

      assert(sdf.schema.toList === List(
        StructField("ex", ArrayType(StringType)),
        StructField("part", IntegerType), // inferred partitionColumn dataType
        StructField("id", StringType))) // used user provided partitionColumn dataType

      val sq = sdf.writeStream
        .queryName("corruption_test")
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        .format("memory")
        .start()
      sq.processAllAvailable()
      checkAnswer(
        spark.table("corruption_test"),
        // notice how `part` is ordered before `id`
        //请注意'id`之前`part`是如何排序
        Row(Array("1"), 0, "0") :: Row(Array("1", "2"), 1, "1") ::
          Row(Array("1", "2", "3"), 2, "2") :: Row(Array("1", "2", "3", "4"), 3, "3") :: Nil
      )
      sq.stop()
    }
  }
  //用户指定的检查点位置在SQLConf之前
  test("user specified checkpointLocation precedes SQLConf") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withTempPath { userCheckpointPath =>
        assert(!userCheckpointPath.exists(), s"$userCheckpointPath should not exist")
        withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
          val queryName = "test_query"
          val ds = MemoryStream[Int].toDS
          ds.writeStream
            // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
            // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
            .format("memory")
            .queryName(queryName)
            .option("checkpointLocation", userCheckpointPath.getAbsolutePath)
            .start()
            .stop()
          assert(checkpointPath.listFiles().isEmpty,
            "SQLConf path is used even if user specified checkpointLoc: " +
              s"${checkpointPath.listFiles()} is not empty")
          assert(userCheckpointPath.exists(),
            s"The user specified checkpointLoc (userCheckpointPath) is not created")
        }
      }
    }
  }
  //未指定checkpointLocation时使用SQLConf检查点目录
  test("use SQLConf checkpoint dir when checkpointLocation is not specified") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
        val queryName = "test_query"
        val ds = MemoryStream[Int].toDS
        // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
        // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        ds.writeStream.format("memory").queryName(queryName).start().stop()
        // Should use query name to create a folder in `checkpointPath`
        val queryCheckpointDir = new File(checkpointPath, queryName)
        assert(queryCheckpointDir.exists(), s"$queryCheckpointDir doesn't exist")
        assert(
          checkpointPath.listFiles().size === 1,
          s"${checkpointPath.listFiles().toList} has 0 or more than 1 files ")
      }
    }
  }
  //没有指定查询名称时,使用checkpointLocation时,请使用SQLConf检查点目录
  test("use SQLConf checkpoint dir when checkpointLocation is not specified without query name") {
    import testImplicits._
    withTempDir { checkpointPath =>
      withSQLConf(SQLConf.CHECKPOINT_LOCATION.key -> checkpointPath.getAbsolutePath) {
        val ds = MemoryStream[Int].toDS
        //控制台接收器（用于调试） - 每次有触发器时将输出打印到控制台/ stdout。
        // 这应该用于低数据量上的调试目的,因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
        ds.writeStream.format("console").start().stop()
        // Should create a random folder in `checkpointPath`
        assert(
          checkpointPath.listFiles().size === 1,
          s"${checkpointPath.listFiles().toList} has 0 or more than 1 files ")
      }
    }
  }
  //如果查询停止没有错误,临时检查点目录应该被删除
  test("temp checkpoint dir should be deleted if a query is stopped without errors") {
    import testImplicits._
    //控制台接收器（用于调试） - 每次有触发器时将输出打印到控制台/ stdout。
    // 这应该用于低数据量上的调试目的,因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
    val query = MemoryStream[Int].toDS.writeStream.format("console").start()
    query.processAllAvailable()
    val checkpointDir = new Path(
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot)
    val fs = checkpointDir.getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(checkpointDir))
    query.stop()
    assert(!fs.exists(checkpointDir))
  }
  //如果查询停止并出现错误,则不应删除临时检查点目录
  testQuietly("temp checkpoint dir should not be deleted if a query is stopped with an error") {
    import testImplicits._
    val input = MemoryStream[Int]
    //控制台接收器（用于调试） - 每次有触发器时将输出打印到控制台/ stdout。
    // 这应该用于低数据量上的调试目的,因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
    val query = input.toDS.map(_ / 0).writeStream.format("console").start()
    val checkpointDir = new Path(
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot)
    val fs = checkpointDir.getFileSystem(spark.sessionState.newHadoopConf())
    assert(fs.exists(checkpointDir))
    input.addData(1)
    intercept[StreamingQueryException] {
      query.awaitTermination()
    }
    assert(fs.exists(checkpointDir))
  }
  //通过使用DDL格式的字符串指定一个模式
  test("SPARK-20431: Specify a schema by using a DDL-formatted string") {
    spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .schema("aa INT")
      .load()

    assert(LastOptions.schema.isDefined)
    assert(LastOptions.schema.get === StructType(StructField("aa", IntegerType) :: Nil))

    LastOptions.clear()
  }
}
