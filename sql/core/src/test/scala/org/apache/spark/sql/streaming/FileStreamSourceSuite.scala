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
import java.net.URI

import scala.util.Random

import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.FileStreamSource.{FileEntry, SeenFilesMap}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.ExistsThrowsExceptionFileSystem._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

abstract class FileStreamSourceTest
  extends StreamTest with SharedSQLContext with PrivateMethodTester {

  import testImplicits._

  /**
   * A subclass `AddData` for adding data to files. This is meant to use the
   * `FileStreamSource` actually being used in the execution.
   */
  abstract class AddFileData extends AddData {
    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active file stream source")

      val sources = getSourcesFromStreamingQuery(query.get)
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find file source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the file source in the StreamExecution logical plan as there" +
            "are multiple file sources:\n\t" + sources.mkString("\n\t"))
      }
      val source = sources.head
      val newOffset = source.withBatchingLocked {
        addData(source)
        new FileStreamSourceOffset(source.currentLogOffset + 1)
      }
      logInfo(s"Added file to $source at offset $newOffset")
      (source, newOffset)
    }

    protected def addData(source: FileStreamSource): Unit
  }

  case class AddTextFileData(content: String, src: File, tmp: File)
    extends AddFileData {

    override def addData(source: FileStreamSource): Unit = {
      val tempFile = Utils.tempFileWith(new File(tmp, "text"))
      val finalFile = new File(src, tempFile.getName)
      src.mkdirs()
      require(stringToFile(tempFile, content).renameTo(finalFile))
      logInfo(s"Written text '$content' to file $finalFile")
    }
  }

  case class AddParquetFileData(data: DataFrame, src: File, tmp: File) extends AddFileData {
    override def addData(source: FileStreamSource): Unit = {
      AddParquetFileData.writeToFile(data, src, tmp)
    }
  }

  object AddParquetFileData {
    def apply(seq: Seq[String], src: File, tmp: File): AddParquetFileData = {
      AddParquetFileData(seq.toDS().toDF(), src, tmp)
    }

    /** Write parquet files in a temp dir, and move the individual files to the 'src' dir
      * 在临时目录中写入parquet文件，并将单个文件移动到“src”目录 */
    def writeToFile(df: DataFrame, src: File, tmp: File): Unit = {
      val tmpDir = Utils.tempFileWith(new File(tmp, "parquet"))
      df.write.parquet(tmpDir.getCanonicalPath)
      src.mkdirs()
      tmpDir.listFiles().foreach { f =>
        f.renameTo(new File(src, s"${f.getName}"))
      }
    }
  }

  /** Use `format` and `path` to create FileStreamSource via DataFrameReader
    * 使用`format`和`path`通过DataFrameReader创建FileStreamSource */
  def createFileStream(
      format: String,
      path: String,
      schema: Option[StructType] = None,
      options: Map[String, String] = Map.empty): DataFrame = {
    val reader =
      if (schema.isDefined) {
        spark.readStream.format(format).schema(schema.get).options(options)
      } else {
        spark.readStream.format(format).options(options)
      }
    reader.load(path)
  }

  protected def getSourceFromFileStream(df: DataFrame): FileStreamSource = {
    val checkpointLocation = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    df.queryExecution.analyzed
      .collect { case StreamingRelation(dataSource, _, _) =>
        // There is only one source in our tests so just set sourceId to 0
        dataSource.createSource(s"$checkpointLocation/sources/0").asInstanceOf[FileStreamSource]
      }.head
  }

  protected def getSourcesFromStreamingQuery(query: StreamExecution): Seq[FileStreamSource] = {
    query.logicalPlan.collect {
      case StreamingExecutionRelation(source, _) if source.isInstanceOf[FileStreamSource] =>
        source.asInstanceOf[FileStreamSource]
    }
  }


  protected def withTempDirs(body: (File, File) => Unit) {
    val src = Utils.createTempDir(namePrefix = "streaming.src")
    val tmp = Utils.createTempDir(namePrefix = "streaming.tmp")
    try {
      body(src, tmp)
    } finally {
      Utils.deleteRecursively(src)
      Utils.deleteRecursively(tmp)
    }
  }

  val valueSchema = new StructType().add("value", StringType)
}

class FileStreamSourceSuite extends FileStreamSourceTest {

  import testImplicits._

  override val streamingTimeout = 20.seconds

  /** Use `format` and `path` to create FileStreamSource via DataFrameReader
    * 使用`format`和`path`通过DataFrameReader创建FileStreamSource */
  private def createFileStreamSource(
      format: String,
      path: String,
      schema: Option[StructType] = None): FileStreamSource = {
    getSourceFromFileStream(createFileStream(format, path, schema))
  }

  private def createFileStreamSourceAndGetSchema(
      format: Option[String],
      path: Option[String],
      schema: Option[StructType] = None): StructType = {
    val reader = spark.readStream
    format.foreach(reader.format)
    schema.foreach(reader.schema)
    val df =
      if (path.isDefined) {
        reader.load(path.get)
      } else {
        reader.load()
      }
    df.queryExecution.analyzed
      .collect { case s @ StreamingRelation(dataSource, _, _) => s.schema }.head
  }

  // ============= Basic parameter exists tests ================
  //
  test("FileStreamSource schema: no path") {
    def testError(): Unit = {
      val e = intercept[IllegalArgumentException] {
        createFileStreamSourceAndGetSchema(format = None, path = None, schema = None)
      }
      assert(e.getMessage.contains("path")) // reason is path, not schema
    }
    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") { testError() }
    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") { testError() }
  }
  //路径不存在（没有模式）应该抛出异常
  test("FileStreamSource schema: path doesn't exist (without schema) should throw exception") {
    withTempDir { dir =>
      intercept[AnalysisException] {
        val userSchema = new StructType().add(new StructField("value", IntegerType))
        val schema = createFileStreamSourceAndGetSchema(
          format = None, path = Some(new File(dir, "1").getAbsolutePath), schema = None)
      }
    }
  }
  //路径不存在（与架构）应该抛出异常
  test("FileStreamSource schema: path doesn't exist (with schema) should throw exception") {
    withTempDir { dir =>
      intercept[AnalysisException] {
        val userSchema = new StructType().add(new StructField("value", IntegerType))
        val schema = createFileStreamSourceAndGetSchema(
          format = None, path = Some(new File(dir, "1").getAbsolutePath), schema = Some(userSchema))
      }
    }
  }


  // =============== Text file stream schema tests ================
  //文本，没有现有的文件，没有架构
  test("FileStreamSource schema: text, no existing files, no schema") {
    withTempDir { src =>
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("text"), path = Some(src.getCanonicalPath), schema = None)
      assert(schema === new StructType().add("value", StringType))
    }
  }
  //文本，现有文件，没有模式
  test("FileStreamSource schema: text, existing files, no schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "a\nb\nc")
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("text"), path = Some(src.getCanonicalPath), schema = None)
      assert(schema === new StructType().add("value", StringType))
    }
  }
  //文本，现有文件，架构
  test("FileStreamSource schema: text, existing files, schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "a\nb\nc")
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("text"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== Parquet file stream schema tests Parquet文件流模式测试 ================
   //现有的文件,没有架构
  test("FileStreamSource schema: parquet, existing files, no schema") {
    withTempDir { src =>
      Seq("a", "b", "c").toDS().as("userColumn").toDF().write
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .parquet(src.getCanonicalPath)

      // Without schema inference, should throw error
      //没有模式推理，应该抛出错误
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") {
        intercept[IllegalArgumentException] {
          createFileStreamSourceAndGetSchema(
            format = Some("parquet"), path = Some(src.getCanonicalPath), schema = None)
        }
      }

      // With schema inference, should infer correct schema
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        val schema = createFileStreamSourceAndGetSchema(
          format = Some("parquet"), path = Some(src.getCanonicalPath), schema = None)
        assert(schema === new StructType().add("value", StringType))
      }
    }
  }
  //parquet，现有的文件，架构
  test("FileStreamSource schema: parquet, existing files, schema") {
    withTempPath { src =>
      Seq("a", "b", "c").toDS().as("oldUserColumn").toDF()
        .write.parquet(new File(src, "1").getCanonicalPath)
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("parquet"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== JSON file stream schema tests  JSON文件流模式测试================
  //JSON,没有现有的文件,没有架构
  test("FileStreamSource schema: json, no existing files, no schema") {
    withTempDir { src =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        val e = intercept[AnalysisException] {
          createFileStreamSourceAndGetSchema(
            format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
        }
        assert("Unable to infer schema for JSON. It must be specified manually.;" === e.getMessage)
      }
    }
  }
  //
  test("FileStreamSource schema: json, existing files, no schema") {
    withTempDir { src =>

      // Without schema inference, should throw error
      //没有模式推理,应该抛出错误
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") {
        intercept[IllegalArgumentException] {
          createFileStreamSourceAndGetSchema(
            format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
        }
      }

      // With schema inference, should infer correct schema
      //使用模式推理,应该推断正确的模式
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        stringToFile(new File(src, "1"), "{'c': '1'}\n{'c': '2'}\n{'c': '3'}")
        val schema = createFileStreamSourceAndGetSchema(
          format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
        assert(schema === new StructType().add("c", StringType))
      }
    }
  }

  test("FileStreamSource schema: json, existing files, schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "{'c': '1'}\n{'c': '2'}\n{'c', '3'}")
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("json"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== Text file stream tests ================
  //从文本文件中读取
  test("read from text files") {
    withTempDirs { case (src, tmp) =>
      val textStream = createFileStream("text", src.getCanonicalPath)
      val filtered = textStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }
  //从文本文件中读取
  test("read from textfile") {
    withTempDirs { case (src, tmp) =>
      val textStream = spark.readStream.textFile(src.getCanonicalPath)
      val filtered = textStream.filter(_.contains("keep"))

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }
  //不应无限期地跟踪看到的文件列表
  test("SPARK-17165 should not track the list of seen files indefinitely") {
    // This test works by:
    // 1. Create a file
    // 2. Get it processed
    // 3. Sleeps for a very short amount of time (larger than maxFileAge
    // 4. Add another file (at this point the original file should have been purged
    // 5. Test the size of the seenFiles internal data structure

    // Note that if we change maxFileAge to a very large number, the last step should fail.
    withTempDirs { case (src, tmp) =>
      val textStream: DataFrame =
        createFileStream("text", src.getCanonicalPath, options = Map("maxFileAge" -> "5ms"))

      testStream(textStream)(
        AddTextFileData("a\nb", src, tmp),
        CheckAnswer("a", "b"),

        // SLeeps longer than 5ms (maxFileAge)
        // Unfortunately since a lot of file system does not have modification time granularity
        // finer grained than 1 sec, we need to use 1 sec here.
        AssertOnQuery { _ => Thread.sleep(1000); true },

        AddTextFileData("c\nd", src, tmp),
        CheckAnswer("a", "b", "c", "d"),

        AssertOnQuery("seen files should contain only one entry") { streamExecution =>
          val source = getSourcesFromStreamingQuery(streamExecution).head
          assert(source.seenFiles.size == 1)
          true
        }
      )
    }
  }

  // =============== JSON file stream tests  JSON文件流测试================
  //从json文件读取
  test("read from json files") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("json", src.getCanonicalPath, Some(valueSchema))
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData(
          "{'value': 'drop1'}\n{'value': 'keep2'}\n{'value': 'keep3'}",
          src,
          tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData(
          "{'value': 'drop4'}\n{'value': 'keep5'}\n{'value': 'keep6'}",
          src,
          tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData(
          "{'value': 'drop7'}\n{'value': 'keep8'}\n{'value': 'keep9'}",
          src,
          tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }
  //从推断模式读取json文件
  test("read from json files with inferring schema") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        //添加一个文件,以便我们可以推断其架构
        stringToFile(new File(src, "existing"), "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}")

        val fileStream = createFileStream("json", src.getCanonicalPath)
        assert(fileStream.schema === StructType(Seq(StructField("c", StringType))))

        // FileStreamSource should infer the column "c"
        val filtered = fileStream.filter($"c" contains "keep")

        testStream(filtered)(
          AddTextFileData("{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6")
        )
      }
    }
  }
  //读取分区目录内的json文件
  test("reading from json files inside partitioned directory") {
    withTempDirs { case (baseSrc, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        val src = new File(baseSrc, "type=X")
        src.mkdirs()

        // Add a file so that we can infer its schema
        //添加一个文件,以便我们可以推断其架构
        stringToFile(new File(src, "existing"), "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}")

        val fileStream = createFileStream("json", src.getCanonicalPath)

        // FileStreamSource should infer the column "c"
        val filtered = fileStream.filter($"c" contains "keep")

        testStream(filtered)(
          AddTextFileData("{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6")
        )
      }
    }
  }
  //从不断变化的模式读取json文件
  test("reading from json files with changing schema") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        //添加一个文件,以便我们可以推断其架构
        stringToFile(new File(src, "existing"), "{'k': 'value0'}")

        val fileStream = createFileStream("json", src.getCanonicalPath)

        // FileStreamSource should infer the column "k"
        //FileStreamSource应该推断列“k”
        assert(fileStream.schema === StructType(Seq(StructField("k", StringType))))

        // After creating DF and before starting stream, add data with different schema
        // Should not affect the inferred schema any more
        stringToFile(new File(src, "existing2"), "{'k': 'value1', 'v': 'new'}")

        testStream(fileStream)(

          // Should not pick up column v in the file added before start
          //开始之前添加的文件中不应该选择列v
          AddTextFileData("{'k': 'value2'}", src, tmp),
          CheckAnswer("value0", "value1", "value2"),

          // Should read data in column k, and ignore v
          //应读取列k中的数据，并忽略v
          AddTextFileData("{'k': 'value3', 'v': 'new'}", src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3"),

          // Should ignore rows that do not have the necessary k column
          //应该忽略没有必要的k列的行
          AddTextFileData("{'v': 'value4'}", src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3", null))
      }
    }
  }

  // =============== Parquet file stream tests  Parquet文件流测试================
  //parquet文件读取
  test("read from parquet files") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("parquet", src.getCanonicalPath, Some(valueSchema))
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddParquetFileData(Seq("drop1", "keep2", "keep3"), src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddParquetFileData(Seq("drop4", "keep5", "keep6"), src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddParquetFileData(Seq("drop7", "keep8", "keep9"), src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }
  //从具有不断变化的模式的parquet文件读取
  test("read from parquet files with changing schema") {

    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        //添加一个文件,以便我们可以推断其架构
        AddParquetFileData.writeToFile(Seq("value0").toDF("k"), src, tmp)

        val fileStream = createFileStream("parquet", src.getCanonicalPath)

        // FileStreamSource should infer the column "k"
        assert(fileStream.schema === StructType(Seq(StructField("k", StringType))))

        // After creating DF and before starting stream, add data with different schema
        // Should not affect the inferred schema any more
        AddParquetFileData.writeToFile(Seq(("value1", 0)).toDF("k", "v"), src, tmp)

        testStream(fileStream)(
          // Should not pick up column v in the file added before start
          //开始之前添加的文件中不应该选择列v
          AddParquetFileData(Seq("value2").toDF("k"), src, tmp),
          CheckAnswer("value0", "value1", "value2"),

          // Should read data in column k, and ignore v
          //应读取列k中的数据，并忽略v
          AddParquetFileData(Seq(("value3", 1)).toDF("k", "v"), src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3"),

          // Should ignore rows that do not have the necessary k column
          //应该忽略没有必要的k列的行
          AddParquetFileData(Seq("value5").toDF("v"), src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3", null)
        )
      }
    }
  }

  // =============== file stream globbing tests  文件流globbing测试================

  test("read new files in nested directories with globbing") {
    withTempDirs { case (dir, tmp) =>

      // src/*/* should consider all the files and directories that matches that glob.
      // So any files that matches the glob as well as any files in directories that matches
      // this glob should be read.
      val fileStream = createFileStream("text", s"${dir.getCanonicalPath}/*/*")
      val filtered = fileStream.filter($"value" contains "keep")
      val subDir = new File(dir, "subdir")
      val subSubDir = new File(subDir, "subsubdir")
      val subSubSubDir = new File(subSubDir, "subsubsubdir")

      require(!subDir.exists())
      require(!subSubDir.exists())

      testStream(filtered)(
        // Create new dir/subdir and write to it, should read
        //创建新的目录/子目录,并写入它,应阅读
        AddTextFileData("drop1\nkeep2", subDir, tmp),
        CheckAnswer("keep2"),

        // Add files to dir/subdir, should read
        //添加文件到dir / subdir,读
        AddTextFileData("keep3", subDir, tmp),
        CheckAnswer("keep2", "keep3"),

        // Create new dir/subdir/subsubdir and write to it, should read
        //创建新的目录/ subdir / subsubdir并写入,读
        AddTextFileData("keep4", subSubDir, tmp),
        CheckAnswer("keep2", "keep3", "keep4"),

        // Add files to dir/subdir/subsubdir, should read
        //添加文件到dir / subdir / subsubdir，读
        AddTextFileData("keep5", subSubDir, tmp),
        CheckAnswer("keep2", "keep3", "keep4", "keep5"),

        // 1. Add file to src dir, should not read as globbing src/*/* does not capture files in
        //    dir, only captures files in dir/subdir/
        // 2. Add files to dir/subDir/subsubdir/subsubsubdir, should not read as src/*/* should
        //    not capture those files
        AddTextFileData("keep6", dir, tmp),
        AddTextFileData("keep7", subSubSubDir, tmp),
        AddTextFileData("keep8", subDir, tmp), // needed to make query detect new data
        CheckAnswer("keep2", "keep3", "keep4", "keep5", "keep8")
      )
    }
  }
  //用globbing在分区表中读取新文件,不应读取分区数据
  test("read new files in partitioned table with globbing, should not read partition data") {
    withTempDirs { case (dir, tmp) =>
      val partitionFooSubDir = new File(dir, "partition=foo")
      val partitionBarSubDir = new File(dir, "partition=bar")

      val schema = new StructType().add("value", StringType).add("partition", StringType)
      val fileStream = createFileStream("json", s"${dir.getCanonicalPath}/*/*", Some(schema))
      val filtered = fileStream.filter($"value" contains "keep")
      val nullStr = null.asInstanceOf[String]
      testStream(filtered)(
        // Create new partition=foo sub dir and write to it, should read only value, not partition
        //创建新的分区= foo子目录并写入它,应该只读取值,而不是分区
        AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", nullStr)),

        // Append to same partition=1 sub dir, should read only value, not partition
        //追加到相同的分区= 1子目录，应该只读值，不分区
        AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", nullStr), ("keep3", nullStr)),

        // Create new partition sub dir and write to it, should read only value, not partition
        //创建新的分区子目录并写入它，应该只读取值，而不是分区
        AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", nullStr), ("keep3", nullStr), ("keep4", nullStr)),

        // Append to same partition=2 sub dir, should read only value, not partition
        AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", nullStr), ("keep3", nullStr), ("keep4", nullStr), ("keep5", nullStr))
      )
    }
  }

  // =============== other tests ================
  //读取分区表中的新文件而不是通配符，应该读取分区数据
  test("read new files in partitioned table without globbing, should read partition data") {
    withTempDirs { case (dir, tmp) =>
      val partitionFooSubDir = new File(dir, "partition=foo")
      val partitionBarSubDir = new File(dir, "partition=bar")

      val schema = new StructType().add("value", StringType).add("partition", StringType)
      val fileStream = createFileStream("json", s"${dir.getCanonicalPath}", Some(schema))
      val filtered = fileStream.filter($"value" contains "keep")
      testStream(filtered)(
        // Create new partition=foo sub dir and write to it
        //创建新的分区= foo子目录并写入它
        AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", "foo")),

        // Append to same partition=foo sub dir
        //附加到相同的分区= foo子目录
        AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo")),

        // Create new partition sub dir and write to it
        //创建新的分区子目录并写入它
      AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar")),

        // Append to same partition=bar sub dir
        //追加到相同的分区=栏子目录
        AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar"))
      )
    }
  }
  //从另一个流式查询的输出中读取数据
  ignore("read data from outputs of another streaming query") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withTempDirs { case (outputDir, checkpointDir) =>
        // q1 is a streaming query that reads from memory and writes to text files
        //q1是一个流式查询，从内存中读取并写入文本文件
        val q1Source = MemoryStream[String]
        val q1 =
          q1Source
            .toDF()
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("text")
            .start(outputDir.getCanonicalPath)

        // q2 is a streaming query that reads q1's text outputs
        //q2是一个流式查询，读取q1的文本输出
        val q2 =
          createFileStream("text", outputDir.getCanonicalPath).filter($"value" contains "keep")

        def q1AddData(data: String*): StreamAction =
          Execute { _ =>
            q1Source.addData(data)
            q1.processAllAvailable()
          }
        def q2ProcessAllAvailable(): StreamAction = Execute { q2 => q2.processAllAvailable() }

        testStream(q2)(
          // batch 0
          q1AddData("drop1", "keep2"),
          q2ProcessAllAvailable(),
          CheckAnswer("keep2"),

          // batch 1
          Assert {
            // create a text file that won't be on q1's sink log
            //创建一个不会出现在q1接收器日志上的文本文件
            // thus even if its content contains "keep", it should NOT appear in q2's answer
            //因此即使其内容包含“keep”，也不应出现在q2的答案中
            val shouldNotKeep = new File(outputDir, "should_not_keep.txt")
            stringToFile(shouldNotKeep, "should_not_keep!!!")
            shouldNotKeep.exists()
          },
          q1AddData("keep3"),
          q2ProcessAllAvailable(),
          CheckAnswer("keep2", "keep3"),

          // batch 2: check that things work well when the sink log gets compacted
          //第2批：检查当日志记录被压缩时，情况是否正常
          q1AddData("keep4"),
          Assert {
            // compact interval is 3, so file "2.compact" should exist
            //紧凑的间隔是3，所以文件“2.compact”应该存在
            new File(outputDir, s"${FileStreamSink.metadataDir}/2.compact").exists()
          },
          q2ProcessAllAvailable(),
          CheckAnswer("keep2", "keep3", "keep4"),

          Execute { _ => q1.stop() }
        )
      }
    }
  }
  //在另一个流式查询之前开始，并读取其输出
  ignore("start before another streaming query, and read its output") {
    withTempDirs { case (outputDir, checkpointDir) =>
      // q1 is a streaming query that reads from memory and writes to text files
      //q1是一个流式查询，从内存中读取并写入文本文件
      val q1Source = MemoryStream[String]
      // define q1, but don't start it for now
      //定义q1，但现在不要启动它
      val q1Write =
        q1Source
          .toDF()
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("text")
      var q1: StreamingQuery = null

      val q2 = createFileStream("text", outputDir.getCanonicalPath).filter($"value" contains "keep")

      testStream(q2)(
        AssertOnQuery { q2 =>
          val fileSource = getSourcesFromStreamingQuery(q2).head
          // q1 has not started yet, verify that q2 doesn't know whether q1 has metadata
          //q1尚未开始，请验证q2是否知道q1是否具有元数据
          fileSource.sourceHasMetadata === None
        },
        Execute { _ =>
          q1 = q1Write.start(outputDir.getCanonicalPath)
          q1Source.addData("drop1", "keep2")
          q1.processAllAvailable()
        },
        AssertOnQuery { q2 =>
          q2.processAllAvailable()
          val fileSource = getSourcesFromStreamingQuery(q2).head
          // q1 has started, verify that q2 knows q1 has metadata by now
          //q1已经开始，现在验证q2知道q1有元数据
          fileSource.sourceHasMetadata === Some(true)
        },
        CheckAnswer("keep2"),
        Execute { _ => q1.stop() }
      )
    }
  }
  //当架构推断开启时，应该读取分区数据
  test("when schema inference is turned on, should read partition data") {
    def createFile(content: String, src: File, tmp: File): Unit = {
      val tempFile = Utils.tempFileWith(new File(tmp, "text"))
      val finalFile = new File(src, tempFile.getName)
      require(!src.exists(), s"$src exists, dir: ${src.isDirectory}, file: ${src.isFile}")
      require(src.mkdirs(), s"Cannot create $src")
      require(src.isDirectory(), s"$src is not a directory")
      require(stringToFile(tempFile, content).renameTo(finalFile))
    }

    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
      withTempDirs { case (dir, tmp) =>
        val partitionFooSubDir = new File(dir, "partition=foo")
        val partitionBarSubDir = new File(dir, "partition=bar")

        // Create file in partition, so we can infer the schema.
        //在分区中创建文件，所以我们可以推断出模式。
        createFile("{'value': 'drop0'}", partitionFooSubDir, tmp)

        val fileStream = createFileStream("json", s"${dir.getCanonicalPath}")
        val filtered = fileStream.filter($"value" contains "keep")
        testStream(filtered)(
          // Append to same partition=foo sub dir
          //附加到相同的分区= foo子目录
          AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
          CheckAnswer(("keep2", "foo")),

          // Append to same partition=foo sub dir
          //附加到相同的分区= foo子目录
          AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo")),

          // Create new partition sub dir and write to it
          //创建新的分区子目录并写入它
          AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar")),

          // Append to same partition=bar sub dir
          AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar")),

          AddTextFileData("{'value': 'keep6'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar"),
            ("keep6", "bar"))
        )
      }
    }
  }
  //容错
  test("fault tolerance") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("text", src.getCanonicalPath)
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }
  //每个触发最大文件数
  test("max files per trigger") {
    withTempDir { case src =>
      var lastFileModTime: Option[Long] = None

      /** Create a text file with a single data item
        * 用一个数据项创建一个文本文件*/
      def createFile(data: Int): File = {
        val file = stringToFile(new File(src, s"$data.txt"), data.toString)
        if (lastFileModTime.nonEmpty) file.setLastModified(lastFileModTime.get + 1000)
        lastFileModTime = Some(file.lastModified)
        file
      }

      createFile(1)
      createFile(2)
      createFile(3)

      // Set up a query to read text files 2 at a time
      //设置查询以一次读取文本文件2
      val df = spark
        .readStream
        .option("maxFilesPerTrigger", 2)
        .text(src.getCanonicalPath)
      val q = df
        .writeStream
        .format("memory")
        .queryName("file_data")
        .start()
        .asInstanceOf[StreamingQueryWrapper]
        .streamingQuery
      q.processAllAvailable()
      val memorySink = q.sink.asInstanceOf[MemorySink]
      val fileSource = getSourcesFromStreamingQuery(q).head

      /** Check the data read in the last batch
        * 检查上一批中读取的数据*/
      def checkLastBatchData(data: Int*): Unit = {
        val schema = StructType(Seq(StructField("value", StringType)))
        val df = spark.createDataFrame(
          spark.sparkContext.makeRDD(memorySink.latestBatchData), schema)
        checkAnswer(df, data.map(_.toString).toDF("value"))
      }

      def checkAllData(data: Seq[Int]): Unit = {
        val schema = StructType(Seq(StructField("value", StringType)))
        val df = spark.createDataFrame(
          spark.sparkContext.makeRDD(memorySink.allData), schema)
        checkAnswer(df, data.map(_.toString).toDF("value"))
      }

      /** Check how many batches have executed since the last time this check was made
        * 检查自上次检查以来执行了多少批次 */
      var lastBatchId = -1L
      def checkNumBatchesSinceLastCheck(numBatches: Int): Unit = {
        require(lastBatchId >= 0)
        assert(memorySink.latestBatchId.get === lastBatchId + numBatches)
        lastBatchId = memorySink.latestBatchId.get
      }

      checkLastBatchData(3)  // (1 and 2) should be in batch 1, (3) should be in batch 2 (last)
      checkAllData(1 to 3)
      lastBatchId = memorySink.latestBatchId.get

      fileSource.withBatchingLocked {
        createFile(4)
        createFile(5)   // 4 and 5 should be in a batch
        createFile(6)
        createFile(7)   // 6 and 7 should be in the last batch
      }
      q.processAllAvailable()
      checkNumBatchesSinceLastCheck(2)
      checkLastBatchData(6, 7)
      checkAllData(1 to 7)

      fileSource.withBatchingLocked {
        createFile(8)
        createFile(9)    // 8 and 9 should be in a batch
        createFile(10)
        createFile(11)   // 10 and 11 should be in a batch
        createFile(12)   // 12 should be in the last batch
      }
      q.processAllAvailable()
      checkNumBatchesSinceLastCheck(3)
      checkLastBatchData(12)
      checkAllData(1 to 12)

      q.stop()
    }
  }
  //每个触发器的最大文件数 - 错误值
  testQuietly("max files per trigger - incorrect values") {
    val testTable = "maxFilesPerTrigger_test"
    withTable(testTable) {
      withTempDir { case src =>
        def testMaxFilePerTriggerValue(value: String): Unit = {
          val df = spark.readStream.option("maxFilesPerTrigger", value).text(src.getCanonicalPath)
          val e = intercept[StreamingQueryException] {
            // Note: `maxFilesPerTrigger` is checked in the stream thread when creating the source
            val q = df.writeStream.format("memory").queryName(testTable).start()
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
          }
          assert(e.getCause.isInstanceOf[IllegalArgumentException])
          Seq("maxFilesPerTrigger", value, "positive integer").foreach { s =>
            assert(e.getMessage.contains(s))
          }
        }

        testMaxFilePerTriggerValue("not-a-integer")
        testMaxFilePerTriggerValue("-1")
        testMaxFilePerTriggerValue("0")
        testMaxFilePerTriggerValue("10.1")
      }
    }
  }
  //说明
  test("explain") {
    withTempDirs { case (src, tmp) =>
      src.mkdirs()

      val df = spark.readStream.format("text").load(src.getCanonicalPath).map(_ + "-x")
      // Test `explain` not throwing errors
      //测试`解释`不要抛出错误
      df.explain()

      val q = df.writeStream.queryName("file_explain").format("memory").start()
        .asInstanceOf[StreamingQueryWrapper]
        .streamingQuery
      try {
        assert("No physical plan. Waiting for data." === q.explainInternal(false))
        assert("No physical plan. Waiting for data." === q.explainInternal(true))

        val tempFile = Utils.tempFileWith(new File(tmp, "text"))
        val finalFile = new File(src, tempFile.getName)
        require(stringToFile(tempFile, "foo").renameTo(finalFile))

        q.processAllAvailable()

        val explainWithoutExtended = q.explainInternal(false)
        // `extended = false` only displays the physical plan.
        assert("Relation.*text".r.findAllMatchIn(explainWithoutExtended).size === 0)
        assert(": Text".r.findAllMatchIn(explainWithoutExtended).size === 1)

        val explainWithExtended = q.explainInternal(true)
        // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
        // plan.
        assert("Relation.*text".r.findAllMatchIn(explainWithExtended).size === 3)
        assert(": Text".r.findAllMatchIn(explainWithExtended).size === 1)
      } finally {
        q.stop()
      }
    }
  }
  //将文件名写入WAL Array [String]
  test("SPARK-17372 - write file names to WAL as Array[String]") {
    // Note: If this test takes longer than the timeout, then its likely that this is actually
    // running a Spark job with 10000 tasks. This test tries to avoid that by
    // 1. Setting the threshold for parallel file listing to very high
    // 2. Using a query that should use constant folding to eliminate reading of the files

    val numFiles = 10000

    // This is to avoid running a spark job to list of files in parallel
    // by the InMemoryFileIndex.
    //这是为了避免通过InMemoryFileIndex并行执行一个Spark作业到文件列表
    spark.sessionState.conf.setConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD, numFiles * 2)

    withTempDirs { case (root, tmp) =>
      val src = new File(root, "a=1")
      src.mkdirs()

      (1 to numFiles).map { _.toString }.foreach { i =>
        val tempFile = Utils.tempFileWith(new File(tmp, "text"))
        val finalFile = new File(src, tempFile.getName)
        stringToFile(finalFile, i)
      }
      assert(src.listFiles().size === numFiles)

      val files = spark.readStream.text(root.getCanonicalPath).as[(String, Int)]

      // Note this query will use constant folding to eliminate the file scan.
      //注意这个查询将使用常量折叠来消除文件扫描
      // This is to avoid actually running a Spark job with 10000 tasks
      //这是为了避免实际运行10000个任务的Spark作业
      val df = files.filter("1 == 0").groupBy().count()

      testStream(df, OutputMode.Complete)(
        AddTextFileData("0", src, tmp),
        CheckAnswer(0)
      )
    }
  }
  //压缩间隔元数据日志
  test("compact interval metadata log") {
    val _sources = PrivateMethod[Seq[Source]]('sources)
    val _metadataLog = PrivateMethod[FileStreamSourceLog]('metadataLog)

    def verify(
        execution: StreamExecution,
        batchId: Long,
        expectedBatches: Int,
        expectedCompactInterval: Int): Boolean = {
      import CompactibleFileStreamLog._

      val fileSource = (execution invokePrivate _sources()).head.asInstanceOf[FileStreamSource]
      val metadataLog = fileSource invokePrivate _metadataLog()

      if (isCompactionBatch(batchId, expectedCompactInterval)) {
        val path = metadataLog.batchIdToPath(batchId)

        // Assert path name should be ended with compact suffix.
        //断言路径名称应以紧凑后缀结尾
        assert(path.getName.endsWith(COMPACT_FILE_SUFFIX),
          "path does not end with compact file suffix")

        // Compacted batch should include all entries from start.
        //压缩批次应包括从开始的所有条目
        val entries = metadataLog.get(batchId)
        assert(entries.isDefined, "Entries not defined")
        assert(entries.get.length === metadataLog.allFiles().length, "clean up check")
        assert(metadataLog.get(None, Some(batchId)).flatMap(_._2).length ===
          entries.get.length, "Length check")
      }

      assert(metadataLog.allFiles().sortBy(_.batchId) ===
        metadataLog.get(None, Some(batchId)).flatMap(_._2).sortBy(_.batchId),
        "Batch id mismatch")

      metadataLog.get(None, Some(batchId)).flatMap(_._2).length === expectedBatches
    }

    withTempDirs { case (src, tmp) =>
      withSQLConf(
        SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2"
      ) {
        val fileStream = createFileStream("text", src.getCanonicalPath)
        val filtered = fileStream.filter($"value" contains "keep")
        val updateConf = Map(SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "5")

        testStream(filtered)(
          AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
          CheckAnswer("keep2", "keep3"),
          AssertOnQuery(verify(_, 0L, 1, 2)),
          AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6"),
          AssertOnQuery(verify(_, 1L, 2, 2)),
          AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9"),
          AssertOnQuery(verify(_, 2L, 3, 2)),
          StopStream,
          StartStream(additionalConfs = updateConf),
          AssertOnQuery(verify(_, 2L, 3, 2)),
          AddTextFileData("drop10\nkeep11", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9", "keep11"),
          AssertOnQuery(verify(_, 3L, 4, 2)),
          AddTextFileData("drop12\nkeep13", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9", "keep11", "keep13"),
          AssertOnQuery(verify(_, 4L, 5, 2))
        )
      }
    }
  }
  //
  test("get arbitrary batch from FileStreamSource") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(
        SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2",
        // Force deleting the old logs 强制删除旧的日志
        SQLConf.FILE_SOURCE_LOG_CLEANUP_DELAY.key -> "1"
      ) {
        val fileStream = createFileStream("text", src.getCanonicalPath)
        val filtered = fileStream.filter($"value" contains "keep")

        testStream(filtered)(
          AddTextFileData("keep1", src, tmp),
          CheckAnswer("keep1"),
          AddTextFileData("keep2", src, tmp),
          CheckAnswer("keep1", "keep2"),
          AddTextFileData("keep3", src, tmp),
          CheckAnswer("keep1", "keep2", "keep3"),
          AssertOnQuery("check getBatch") { execution: StreamExecution =>
            val _sources = PrivateMethod[Seq[Source]]('sources)
            val fileSource =
              (execution invokePrivate _sources()).head.asInstanceOf[FileStreamSource]

            def verify(startId: Option[Int], endId: Int, expected: String*): Unit = {
              val start = startId.map(new FileStreamSourceOffset(_))
              val end = FileStreamSourceOffset(endId)

              withSQLConf("spark.sql.streaming.unsupportedOperationCheck" -> "false") {
                assert(fileSource.getBatch(start, end).as[String].collect().toSeq === expected)
              }
            }

            verify(startId = None, endId = 2, "keep1", "keep2", "keep3")
            verify(startId = Some(0), endId = 1, "keep2")
            verify(startId = Some(0), endId = 2, "keep2", "keep3")
            verify(startId = Some(1), endId = 2, "keep3")
            true
          }
        )
      }
    }
  }
  //输入行度量
  test("input row metrics") {
    withTempDirs { case (src, tmp) =>
      val input = spark.readStream.format("text").load(src.getCanonicalPath)
      testStream(input)(
        AddTextFileData("100", src, tmp),
        CheckAnswer("100"),
        AssertOnQuery { query =>
          val actualProgress = query.recentProgress
              .find(_.numInputRows > 0)
              .getOrElse(sys.error("Could not find records with data."))
          assert(actualProgress.numInputRows === 1)
          assert(actualProgress.sources(0).processedRowsPerSecond > 0.0)
          true
        }
      )
    }
  }
  //将DataSource选项键改为更不区分大小写
  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new FileStreamOptions(Map("maxfilespertrigger" -> "1"))
    assert(options.maxFilesPerTrigger == Some(1))
  }
  //FileStreamSource offset - 读取Spark 2.1.0偏移量的json格式
  test("FileStreamSource offset - read Spark 2.1.0 offset json format") {
    val offset = readOffsetFromResource("file-source-offset-version-2.1.0-json.txt")
    assert(FileStreamSourceOffset(offset) === FileStreamSourceOffset(345))
  }
  //FileStreamSource offset - 读取Spark 2.1.0偏移长格式
  test("FileStreamSource offset - read Spark 2.1.0 offset long format") {
    val offset = readOffsetFromResource("file-source-offset-version-2.1.0-long.txt")
    assert(FileStreamSourceOffset(offset) === FileStreamSourceOffset(345))
  }
  //FileStreamSourceLog - 读取Spark 2.1.0日志格式
  test("FileStreamSourceLog - read Spark 2.1.0 log format") {
    assert(readLogFromResource("file-source-log-version-2.1.0") === Seq(
      FileEntry("/a/b/0", 1480730949000L, 0L),
      FileEntry("/a/b/1", 1480730950000L, 1L),
      FileEntry("/a/b/2", 1480730950000L, 2L),
      FileEntry("/a/b/3", 1480730950000L, 3L),
      FileEntry("/a/b/4", 1480730951000L, 4L)
    ))
  }

  private def readLogFromResource(dir: String): Seq[FileEntry] = {
    val input = getClass.getResource(s"/structured-streaming/$dir")
    val log = new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark, input.toString)
    log.allFiles()
  }

  private def readOffsetFromResource(file: String): SerializedOffset = {
    import scala.io.Source
    val str = Source.fromFile(getClass.getResource(s"/structured-streaming/$file").toURI).mkString
    SerializedOffset(str.trim)
  }

  private def runTwoBatchesAndVerifyResults(
      src: File,
      latestFirst: Boolean,
      firstBatch: String,
      secondBatch: String,
      maxFileAge: Option[String] = None): Unit = {
    val srcOptions = Map("latestFirst" -> latestFirst.toString, "maxFilesPerTrigger" -> "1") ++
      maxFileAge.map("maxFileAge" -> _)
    val fileStream = createFileStream(
      "text",
      src.getCanonicalPath,
      options = srcOptions)
    val clock = new StreamManualClock()
    testStream(fileStream)(
      StartStream(trigger = ProcessingTime(10), triggerClock = clock),
      AssertOnQuery { _ =>
        // Block until the first batch finishes.
        eventually(timeout(streamingTimeout)) {
          assert(clock.isStreamWaitingAt(0))
        }
        true
      },
      CheckLastBatch(firstBatch),
      AdvanceManualClock(10),
      AssertOnQuery { _ =>
        // Block until the second batch finishes.
        eventually(timeout(streamingTimeout)) {
          assert(clock.isStreamWaitingAt(10))
        }
        true
      },
      CheckLastBatch(secondBatch)
    )
  }
  //
  ignore("FileStreamSource - latestFirst") {
    withTempDir { src =>
      // Prepare two files: 1.txt, 2.txt, and make sure they have different modified time.
      //准备两个文件：1.txt，2.txt，并确保它们有不同的修改时间。
      val f1 = stringToFile(new File(src, "1.txt"), "1")
      val f2 = stringToFile(new File(src, "2.txt"), "2")
      f2.setLastModified(f1.lastModified + 1000)

      // Read oldest files first, so the first batch is "1", and the second batch is "2".
      //先读取最旧的文件，所以第一批是“1”，第二批是“2”。
      runTwoBatchesAndVerifyResults(src, latestFirst = false, firstBatch = "1", secondBatch = "2")

      // Read latest files first, so the first batch is "2", and the second batch is "1".
      //先读取最新的文件，第一批为“2”，第二批为“1”
      runTwoBatchesAndVerifyResults(src, latestFirst = true, firstBatch = "2", secondBatch = "1")
    }
  }
  //当使用maxFilesPerTrigger和latestFirst时，忽略maxFileAge
  ignore("SPARK-19813: Ignore maxFileAge when maxFilesPerTrigger and latestFirst is used") {
    withTempDir { src =>
      // Prepare two files: 1.txt, 2.txt, and make sure they have different modified time.
      //准备两个文件：1.txt，2.txt，并确保它们有不同的修改时间
      val f1 = stringToFile(new File(src, "1.txt"), "1")
      val f2 = stringToFile(new File(src, "2.txt"), "2")
      f2.setLastModified(f1.lastModified + 3600 * 1000 /* 1 hour later */)

      runTwoBatchesAndVerifyResults(src, latestFirst = true, firstBatch = "2", secondBatch = "1",
        maxFileAge = Some("1m") /* 1 minute */)
    }
  }

  test("SeenFilesMap") {
    val map = new SeenFilesMap(maxAgeMs = 10, fileNameOnly = false)

    map.add("a", 5)
    assert(map.size == 1)
    map.purge()
    assert(map.size == 1)

    // Add a new entry and purge should be no-op, since the gap is exactly 10 ms.
    //添加一个新条目，清除应该是no-op，因为间隔恰好是10毫秒
    map.add("b", 15)
    assert(map.size == 2)
    map.purge()
    assert(map.size == 2)

    // Add a new entry that's more than 10 ms than the first entry. We should be able to purge now.
    //添加比第一个条目多10毫秒的新条目,我们现在应该能够清除。
    map.add("c", 16)
    assert(map.size == 3)
    map.purge()
    assert(map.size == 2)

    // Override existing entry shouldn't change the size
    //覆盖现有的条目不应该改变大小
    map.add("c", 25)
    assert(map.size == 2)

    // Not a new file because we have seen c before
    //不是新文件，因为我们以前见过c
    assert(!map.isNewFile("c", 20))

    // Not a new file because timestamp is too old
    //不是新文件,因为时间戳太旧
    assert(!map.isNewFile("d", 5))

    // Finally a new file: never seen and not too old
    //最后一个新的文件：从来没有见过,也不是太旧
    assert(map.isNewFile("e", 20))
  }

  test("SeenFilesMap with fileNameOnly = true") {
    val map = new SeenFilesMap(maxAgeMs = 10, fileNameOnly = true)

    map.add("file:///a/b/c/d", 5)
    map.add("file:///a/b/c/e", 5)
    assert(map.size === 2)

    assert(!map.isNewFile("d", 5))
    assert(!map.isNewFile("file:///d", 5))
    assert(!map.isNewFile("file:///x/d", 5))
    assert(!map.isNewFile("file:///x/y/d", 5))

    map.add("s3:///bucket/d", 5)
    map.add("s3n:///bucket/d", 5)
    map.add("s3a:///bucket/d", 5)
    assert(map.size === 2)
  }
  //应该只考虑一个旧的文件，如果它早于最后的清除时间
  test("SeenFilesMap should only consider a file old if it is earlier than last purge time") {
    val map = new SeenFilesMap(maxAgeMs = 10, fileNameOnly = false)

    map.add("a", 20)
    assert(map.size == 1)

    // Timestamp 5 should still considered a new file because purge time should be 0
    //时间戳5仍然应该考虑一个新的文件，因为清除时间应该是0
    assert(map.isNewFile("b", 9))
    assert(map.isNewFile("b", 10))

    // Once purge, purge time should be 10 and then b would be a old file if it is less than 10.
    map.purge()
    assert(!map.isNewFile("b", 9))
    assert(map.isNewFile("b", 10))
  }
  //在getBatch期间不要重新检查文件是否存在
  test("do not recheck that files exist during getBatch") {
    withTempDir { temp =>
      spark.conf.set(
        s"fs.$scheme.impl",
        classOf[ExistsThrowsExceptionFileSystem].getName)
      // add the metadata entries as a pre-req
      //将元数据条目添加为预先请求
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
    val metadataLog =
      new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark, dir.getAbsolutePath)
      assert(metadataLog.add(0, Array(FileEntry(s"$scheme:///file1", 100L, 0))))
      assert(metadataLog.add(1, Array(FileEntry(s"$scheme:///file2", 200L, 0))))

      val newSource = new FileStreamSource(spark, s"$scheme:///", "parquet", StructType(Nil), Nil,
        dir.getAbsolutePath, Map.empty)
      // this method should throw an exception if `fs.exists` is called during resolveRelation
      //如果在resolveRelation中调用fs.exists，这个方法应该会抛出一个异常
      newSource.getBatch(None, FileStreamSourceOffset(1))
    }
  }
}

class FileStreamSourceStressTestSuite extends FileStreamSourceTest {

  import testImplicits._

  testQuietly("file source stress test") {
    val src = Utils.createTempDir(namePrefix = "streaming.src")
    val tmp = Utils.createTempDir(namePrefix = "streaming.tmp")

    val fileStream = createFileStream("text", src.getCanonicalPath)
    val ds = fileStream.as[String].map(_.toInt + 1)
    runStressTest(ds, data => {
      AddTextFileData(data.mkString("\n"), src, tmp)
    })

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }
}

/**
 * Fake FileSystem to test whether the method `fs.exists` is called during
 * `DataSource.resolveRelation`.
 */
class ExistsThrowsExceptionFileSystem extends RawLocalFileSystem {
  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def exists(f: Path): Boolean = {
    throw new IllegalArgumentException("Exists shouldn't have been called!")
  }

  /** Simply return an empty file for now.
    * 现在只需返回一个空文件*/
  override def listStatus(file: Path): Array[FileStatus] = {
    val emptyFile = new FileStatus()
    emptyFile.setPath(file)
    Array(emptyFile)
  }
}

object ExistsThrowsExceptionFileSystem {
  val scheme = s"FileStreamSourceSuite${math.abs(Random.nextInt)}fs"
}
