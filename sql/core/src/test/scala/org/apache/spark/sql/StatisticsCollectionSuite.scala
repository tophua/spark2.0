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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.SQLTestData.ArrayData
import org.apache.spark.sql.types._


/**
 * End-to-end suite testing statistics collection and use on both entire table and columns.
  * 端到端套件测试统计信息在整个表和列上进行收集和使用
 */
class StatisticsCollectionSuite extends StatisticsCollectionTestBase with SharedSQLContext {
  import testImplicits._
  //估计外连接上的限制0的大小
  test("estimates the size of a limit 0 on outer join") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      val df1 = spark.table("test")
      val df2 = spark.table("test").limit(0)
      val df = df1.join(df2, Seq("k"), "left")

      val sizes = df.queryExecution.analyzed.collect { case g: Join =>
        g.stats.sizeInBytes
      }

      assert(sizes.size === 1, s"number of Join nodes is wrong:\n ${df.queryExecution}")
      assert(sizes.head === BigInt(96),
        s"expected exact size 96 for table 'test', got: ${sizes.head}")
    }
  }
  //不支持分析视图
  test("analyzing views is not supported") {
    def assertAnalyzeUnsupported(analyzeCommand: String): Unit = {
      val err = intercept[AnalysisException] {
        sql(analyzeCommand)
      }
      assert(err.message.contains("ANALYZE TABLE is not supported"))
    }

    val tableName = "tbl"
    withTable(tableName) {
      spark.range(10).write.saveAsTable(tableName)
      val viewName = "view"
      withView(viewName) {
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $tableName")
        assertAnalyzeUnsupported(s"ANALYZE TABLE $viewName COMPUTE STATISTICS")
        assertAnalyzeUnsupported(s"ANALYZE TABLE $viewName COMPUTE STATISTICS FOR COLUMNS id")
      }
    }
  }
  //一个表的统计数据收集与零列
  test("statistics collection of a table with zero column") {
    val table_no_cols = "table_no_cols"
    withTable(table_no_cols) {
      val rddNoCols = sparkContext.parallelize(1 to 10).map(_ => Row.empty)
      val dfNoCols = spark.createDataFrame(rddNoCols, StructType(Seq.empty))
      dfNoCols.write.format("json").saveAsTable(table_no_cols)
      sql(s"ANALYZE TABLE $table_no_cols COMPUTE STATISTICS")
      checkTableStats(table_no_cols, hasSizeInBytes = true, expectedRowCounts = Some(10))
    }
  }
  //分析空表
  test("analyze empty table") {
    val table = "emptyTable"
    withTable(table) {
      sql(s"CREATE TABLE $table (key STRING, value STRING) USING PARQUET")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS noscan")
      val fetchedStats1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
      assert(fetchedStats1.get.sizeInBytes == 0)
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      val fetchedStats2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
      assert(fetchedStats2.get.sizeInBytes == 0)
    }
  }
  //分析列命令 - 不支持的类型和无效的列
  test("analyze column command - unsupported types and invalid columns") {
    val tableName = "column_stats_test1"
    withTable(tableName) {
      Seq(ArrayData(Seq(1, 2, 3), Seq(Seq(1, 2, 3)))).toDF().write.saveAsTable(tableName)

      // Test unsupported data types
      //测试不支持的数据类型
      val err1 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS data")
      }
      assert(err1.message.contains("does not support statistics collection"))

      // Test invalid columns
      //测试无效的列
      val err2 = intercept[AnalysisException] {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS some_random_column")
      }
      assert(err2.message.contains("does not exist"))
    }
  }
  //测试数据源表的表级别统计信息
  test("test table-level statistics for data source table") {
    val tableName = "tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i INT, j STRING) USING parquet")
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto(tableName)

      // noscan won't count the number of rows
      //noscan不会计算行数
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
      checkTableStats(tableName, hasSizeInBytes = true, expectedRowCounts = None)

      // without noscan, we count the number of rows
      //没有noscan，我们计算行数
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      checkTableStats(tableName, hasSizeInBytes = true, expectedRowCounts = Some(2))
    }
  }
  //从RDD创建的数据帧不应该被广播
  test("SPARK-15392: DataFrame created from RDD should not be broadcasted") {
    val rdd = sparkContext.range(1, 100).map(i => Row(i, i))
    val df = spark.createDataFrame(rdd, new StructType().add("a", LongType).add("b", LongType))
    assert(df.queryExecution.analyzed.stats.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
    assert(df.selectExpr("a").queryExecution.analyzed.stats.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
  }
  //列统计往返序列化
  test("column stats round trip serialization") {
    // Make sure we serialize and then deserialize and we will get the result data
    //确保我们序列化，然后反序列化，我们将得到结果数据
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    stats.zip(df.schema).foreach { case ((k, v), field) =>
      withClue(s"column $k with type ${field.dataType}") {
        val roundtrip = ColumnStat.fromMap("table_is_foo", field, v.toMap(k, field.dataType))
        assert(roundtrip == Some(v))
      }
    }
  }
  //analyze column command - result verification
  test("analyze column command - result verification") {
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)
    checkColStats(df, stats)
  }
  //空列的列统计信息收集
  test("column stats collection for null columns") {
    val dataTypes: Seq[(DataType, Int)] = Seq(
      BooleanType, ByteType, ShortType, IntegerType, LongType,
      DoubleType, FloatType, DecimalType.SYSTEM_DEFAULT,
      StringType, BinaryType, DateType, TimestampType
    ).zipWithIndex

    val df = sql("select " + dataTypes.map { case (tpe, idx) =>
      s"cast(null as ${tpe.sql}) as col$idx"
    }.mkString(", "))

    val expectedColStats = dataTypes.map { case (tpe, idx) =>
      (s"col$idx", ColumnStat(0, None, None, 1, tpe.defaultSize.toLong, tpe.defaultSize.toLong))
    }
    checkColStats(df, mutable.LinkedHashMap(expectedColStats: _*))
  }
  //统计中的数字格式
  test("number format in statistics") {
    val numbers = Seq(
      BigInt(0) -> (("0.0 B", "0")),
      BigInt(100) -> (("100.0 B", "100")),
      BigInt(2047) -> (("2047.0 B", "2.05E+3")),
      BigInt(2048) -> (("2.0 KB", "2.05E+3")),
      BigInt(3333333) -> (("3.2 MB", "3.33E+6")),
      BigInt(4444444444L) -> (("4.1 GB", "4.44E+9")),
      BigInt(5555555555555L) -> (("5.1 TB", "5.56E+12")),
      BigInt(6666666666666666L) -> (("5.9 PB", "6.67E+15")),
      BigInt(1L << 10 ) * (1L << 60) -> (("1024.0 EB", "1.18E+21")),
      BigInt(1L << 11) * (1L << 60) -> (("2.36E+21 B", "2.36E+21"))
    )
    numbers.foreach { case (input, (expectedSize, expectedRows)) =>
      val stats = Statistics(sizeInBytes = input, rowCount = Some(input))
      val expectedString = s"sizeInBytes=$expectedSize, rowCount=$expectedRows," +
        s" hints=none"
      assert(stats.simpleString == expectedString)
    }
  }
  //截断命令后更改统计信息
  test("change stats after truncate command") {
    val table = "change_stats_truncate_table"
    withTable(table) {
      spark.range(100).select($"id", $"id" % 5 as "value").write.saveAsTable(table)
      // analyze to get initial stats
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS id, value")
      val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(100))
      assert(fetched1.get.sizeInBytes > 0)
      assert(fetched1.get.colStats.size == 2)

      // truncate table command
      sql(s"TRUNCATE TABLE $table")
      val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
      assert(fetched2.get.sizeInBytes == 0)
      assert(fetched2.get.colStats.isEmpty)
    }
  }
  //在设置位置命令后更改统计信息
  test("change stats after set location command") {
    val table = "change_stats_set_location_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_UPDATE_SIZE.key -> autoUpdate.toString) {
        withTable(table) {
          spark.range(100).select($"id", $"id" % 5 as "value").write.saveAsTable(table)
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS id, value")
          val fetched1 = checkTableStats(
            table, hasSizeInBytes = true, expectedRowCounts = Some(100))
          assert(fetched1.get.sizeInBytes > 0)
          assert(fetched1.get.colStats.size == 2)

          // set location command 设置位置命令
          val initLocation = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table))
            .storage.locationUri.get.toString
          withTempDir { newLocation =>
            sql(s"ALTER TABLE $table SET LOCATION '${newLocation.toURI.toString}'")
            if (autoUpdate) {
              val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched2.get.sizeInBytes == 0)
              assert(fetched2.get.colStats.isEmpty)

              // set back to the initial location 设置回初始位置
              sql(s"ALTER TABLE $table SET LOCATION '$initLocation'")
              val fetched3 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched3.get.sizeInBytes == fetched1.get.sizeInBytes)
            } else {
              checkTableStats(table, hasSizeInBytes = false, expectedRowCounts = None)
            }
          }
        }
      }
    }
  }
  //在为数据源表插入命令后更改统计信息
  test("change stats after insert command for datasource table") {
    val table = "change_stats_insert_datasource_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_UPDATE_SIZE.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i int, j string) USING PARQUET")
          // analyze to get initial stats
          // 分析得到最初的统计数据
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
          val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
          assert(fetched1.get.sizeInBytes == 0)
          assert(fetched1.get.colStats.size == 2)

          // insert into command 插入命令
          sql(s"INSERT INTO TABLE $table SELECT 1, 'abc'")
          if (autoUpdate) {
            val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
            assert(fetched2.get.sizeInBytes > 0)
            assert(fetched2.get.colStats.isEmpty)
          } else {
            checkTableStats(table, hasSizeInBytes = false, expectedRowCounts = None)
          }
        }
      }
    }
  }

}
