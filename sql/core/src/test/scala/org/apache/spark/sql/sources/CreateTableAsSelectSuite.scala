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

package org.apache.spark.sql.sources

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class CreateTableAsSelectSuite
  extends DataSourceTest
  with SharedSQLContext
  with BeforeAndAfterEach {
  import testImplicits._

  protected override lazy val sql = spark.sql _
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val ds = (1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}""").toDS()
    spark.read.json(ds).createOrReplaceTempView("jt")
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("jt")
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    path = Utils.createTempDir()
    path.delete()
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(path)
    super.afterEach()
  }
  //使用AS SELECT来创建表
  test("CREATE TABLE USING AS SELECT") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT a, b FROM jsonTable"),
        sql("SELECT a, b FROM jt"))
    }
  }
  //使用AS SELECT根据没有写入权限的文件创建表
  test("CREATE TABLE USING AS SELECT based on the file without write permission") {
    // setWritable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)
    val childPath = new File(path.toString, "child")
    path.mkdir()
    path.setWritable(false)

    val e = intercept[SparkException] {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${childPath.toURI}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)
      sql("SELECT a, b FROM jsonTable").collect()
    }

    assert(e.getMessage().contains("Job aborted"))
    path.setWritable(true)
  }
  //创建一个表,将其放置并创建另一个具有相同名称的表
  test("create a table, drop it and create another one with the same name") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT a, b FROM jsonTable"),
        sql("SELECT a, b FROM jt"))

      // Creates a table of the same name with flag "if not exists", nothing happens
      //用标志“如果不存在”创建一个同名的表,什么都不会发生
      sql(
        s"""
           |CREATE TABLE IF NOT EXISTS jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT a * 4 FROM jt
         """.stripMargin)
      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        sql("SELECT a, b FROM jt"))

      // Explicitly drops the table and deletes the underlying data.
      //显式删除表并删除基础数据
      sql("DROP TABLE jsonTable")
      if (path.exists()) Utils.deleteRecursively(path)

      // Creates a table of the same name again, this time we succeed.
      //再次创建一个同名的表,这次我们成功了
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toURI}'
           |) AS
           |SELECT b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        sql("SELECT b FROM jt"))
    }
  }
  //不允许CREATE TEMPORARY TABLE ... USING ... AS查询
  test("disallows CREATE TEMPORARY TABLE ... USING ... AS query") {
    withTable("t") {
      val error = intercept[ParseException] {
        sql(
          s"""
             |CREATE TEMPORARY TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toURI}')
             |PARTITIONED BY (a)
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage
      assert(error.contains("Operation not allowed") &&
        error.contains("CREATE TEMPORARY TABLE ... USING ... AS query"))
    }
  }
  //不允许CREATE EXTERNAL TABLE ... USING ... AS查询
  test("disallows CREATE EXTERNAL TABLE ... USING ... AS query") {
    withTable("t") {
      val error = intercept[ParseException] {
        sql(
          s"""
             |CREATE EXTERNAL TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toURI}')
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage

      assert(error.contains("Operation not allowed") &&
        error.contains("CREATE EXTERNAL TABLE ... USING"))
    }
  }
  //创建表使用选择 - 分区
  test("create table using as select - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |PARTITIONED BY (a)
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.partitionColumnNames == Seq("a"))
    }
  }
  //使用select来创建表 - 使用有效数量的桶
  test("create table using as select - with valid number of buckets") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.bucketSpec == Option(BucketSpec(5, Seq("a"), Seq("b"))))
    }
  }
  //创建表使用选择 - 与无效数量的桶
  test("create table using as select - with invalid number of buckets") {
    withTable("t") {
      Seq(0, 100000).foreach(numBuckets => {
        val e = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TABLE t USING PARQUET
               |OPTIONS (PATH '${path.toURI}')
               |CLUSTERED BY (a) SORTED BY (b) INTO $numBuckets BUCKETS
               |AS SELECT 1 AS a, 2 AS b
             """.stripMargin
          )
        }.getMessage
        assert(e.contains("Number of buckets should be greater than 0 but less than 100000"))
      })
    }
  }

  test("SPARK-17409: CTAS of decimal calculation") {
    withTable("tab2") {
      withTempView("tab1") {
        spark.range(99, 101).createOrReplaceTempView("tab1")
        val sqlStmt =
          "SELECT id, cast(id as long) * cast('1.0' as decimal(38, 18)) as num FROM tab1"
        sql(s"CREATE TABLE tab2 USING PARQUET AS $sqlStmt")
        checkAnswer(spark.table("tab2"), sql(sqlStmt))
      }
    }
  }

  test("specifying the column list for CTAS") {
    withTable("t") {
      val e = intercept[ParseException] {
        sql("CREATE TABLE t (a int, b int) USING parquet AS SELECT 1, 2")
      }.getMessage
      assert(e.contains("Schema may not be specified in a Create Table As Select (CTAS)"))
    }
  }
}
