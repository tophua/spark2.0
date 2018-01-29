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
import java.net.URI

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class BucketedWriteWithoutHiveSupportSuite extends BucketedWriteSuite with SharedSQLContext {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
  }

  override protected def fileFormatsToTest: Seq[String] = Seq("parquet", "json")
}

abstract class BucketedWriteSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected def fileFormatsToTest: Seq[String]
  //被不存在的柱子撑起来
  test("bucketed by non-existing column") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "k").saveAsTable("tt"))
  }
  //numBuckets大于0但小于100000
  test("numBuckets be greater than 0 but less than 100000") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    Seq(-1, 0, 100000).foreach(numBuckets => {
      val e = intercept[AnalysisException](df.write.bucketBy(numBuckets, "i").saveAsTable("tt"))
      assert(
        e.getMessage.contains("Number of buckets should be greater than 0 but less than 100000"))
    })
  }
  //指定没有分组列的排序列
  test("specify sorting columns without bucketing columns") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.sortBy("j").saveAsTable("tt"))
  }
  //通过不可订购的列进行排序
  test("sorting by non-orderable column") {
    val df = Seq("a" -> Map(1 -> 1), "b" -> Map(2 -> 2)).toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "i").sortBy("j").saveAsTable("tt"))
  }
  //使用save（）编写bucketed数据
  test("write bucketed data using save()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").parquet("/tmp/path")
    }
    assert(e.getMessage == "'save' does not support bucketing right now;")
  }
  //使用insertInto（）写入bucketed数据
  test("write bucketed data using insertInto()") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")

    val e = intercept[AnalysisException] {
      df.write.bucketBy(2, "i").insertInto("tt")
    }
    assert(e.getMessage == "'insertInto' does not support bucketing right now;")
  }

  private lazy val df = {
    (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
  }

  def tableDir: File = {
    val identifier = spark.sessionState.sqlParser.parseTableIdentifier("bucketed_table")
    new File(spark.sessionState.catalog.defaultTablePath(identifier))
  }

  /**
   * A helper method to check the bucket write functionality in low level, i.e. check the written
   * bucket files to see if the data are correct.  User should pass in a data dir that these bucket
   * files are written to, and the format of data(parquet, json, etc.), and the bucketing
   * information.
    * 辅助方法检查低级别的桶写入功能,即检查写入的桶文件以查看数据是否正确。
    * 用户应该传入这些桶文件所写入的数据目录,以及数据格式（parquet，json等）和分段信息。
   */
  private def testBucketing(
      dataDir: File,
      source: String,
      numBuckets: Int,
      bucketCols: Seq[String],
      sortCols: Seq[String] = Nil): Unit = {
    val allBucketFiles = dataDir.listFiles().filterNot(f =>
      f.getName.startsWith(".") || f.getName.startsWith("_")
    )

    for (bucketFile <- allBucketFiles) {
      val bucketId = BucketingUtils.getBucketId(bucketFile.getName).getOrElse {
        fail(s"Unable to find the related bucket files.")
      }

      // Remove the duplicate columns in bucketCols and sortCols;
      //删除bucketCols和sortCols中的重复列
      // Otherwise, we got analysis errors due to duplicate names
      //否则,由于名称重复,我们会得到分析错误
      val selectedColumns = (bucketCols ++ sortCols).distinct
      // We may lose the type information after write(e.g. json format doesn't keep schema
      // information), here we get the types from the original dataframe.
      val types = df.select(selectedColumns.map(col): _*).schema.map(_.dataType)
      val columns = selectedColumns.zip(types).map {
        case (colName, dt) => col(colName).cast(dt)
      }

      // Read the bucket file into a dataframe, so that it's easier to test.
      //将桶文件读入数据框,以便测试
      val readBack = spark.read.format(source)
        .load(bucketFile.getAbsolutePath)
        .select(columns: _*)

      // If we specified sort columns while writing bucket table, make sure the data in this
      // bucket file is already sorted.
      //如果我们在写入桶表时指定了排序列,请确保此桶文件中的数据已经排序
      if (sortCols.nonEmpty) {
        checkAnswer(readBack.sort(sortCols.map(col): _*), readBack.collect())
      }

      // Go through all rows in this bucket file, calculate bucket id according to bucket column
      // values, and make sure it equals to the expected bucket id that inferred from file name.
      //遍历此桶文件中的所有行,根据桶列值计算桶ID,并确保它等于从文件名推断出的期望桶ID
      val qe = readBack.select(bucketCols.map(col): _*).queryExecution
      val rows = qe.toRdd.map(_.copy()).collect()
      val getBucketId = UnsafeProjection.create(
        HashPartitioning(qe.analyzed.output, numBuckets).partitionIdExpression :: Nil,
        qe.analyzed.output)

      for (row <- rows) {
        val actualBucketId = getBucketId(row).getInt(0)
        assert(actualBucketId == bucketId)
      }
    }
  }
  //写桶装数据
  test("write bucketed data") {
    for (source <- fileFormatsToTest) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .partitionBy("i")
          .bucketBy(8, "j", "k")
          .saveAsTable("bucketed_table")

        for (i <- 0 until 5) {
          testBucketing(new File(tableDir, s"i=$i"), source, 8, Seq("j", "k"))
        }
      }
    }
  }
  //用sortBy写bucketed数据
  test("write bucketed data with sortBy") {
    for (source <- fileFormatsToTest) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .partitionBy("i")
          .bucketBy(8, "j")
          .sortBy("k")
          .saveAsTable("bucketed_table")

        for (i <- 0 until 5) {
          testBucketing(new File(tableDir, s"i=$i"), source, 8, Seq("j"), Seq("k"))
        }
      }
    }
  }
  //用重叠的bucketBy / sortBy和partitionBy列写入分段数据
  test("write bucketed data with the overlapping bucketBy/sortBy and partitionBy columns") {
    val e1 = intercept[AnalysisException](df.write
      .partitionBy("i", "j")
      .bucketBy(8, "j", "k")
      .sortBy("k")
      .saveAsTable("bucketed_table"))
    assert(e1.message.contains("bucketing column 'j' should not be part of partition columns"))

    val e2 = intercept[AnalysisException](df.write
      .partitionBy("i", "j")
      .bucketBy(8, "k")
      .sortBy("i")
      .saveAsTable("bucketed_table"))
    assert(e2.message.contains("bucket sorting column 'i' should not be part of partition columns"))
  }
  //写没有partitionBy的bucketed数据
  test("write bucketed data without partitionBy") {
    for (source <- fileFormatsToTest) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .bucketBy(8, "i", "j")
          .saveAsTable("bucketed_table")

        testBucketing(tableDir, source, 8, Seq("i", "j"))
      }
    }
  }
  //使用sortBy编写没有分区的bucketed数据
  test("write bucketed data without partitionBy with sortBy") {
    for (source <- fileFormatsToTest) {
      withTable("bucketed_table") {
        df.write
          .format(source)
          .bucketBy(8, "i", "j")
          .sortBy("k")
          .saveAsTable("bucketed_table")

        testBucketing(tableDir, source, 8, Seq("i", "j"), Seq("k"))
      }
    }
  }
  //写入bucketed数据禁用bucketing
  test("write bucketed data with bucketing disabled") {
    // The configuration BUCKETING_ENABLED does not affect the writing path
    //配置BUCKETING_ENABLED不影响写入路径
    withSQLConf(SQLConf.BUCKETING_ENABLED.key -> "false") {
      for (source <- fileFormatsToTest) {
        withTable("bucketed_table") {
          df.write
            .format(source)
            .partitionBy("i")
            .bucketBy(8, "j", "k")
            .saveAsTable("bucketed_table")

          for (i <- 0 until 5) {
            testBucketing(new File(tableDir, s"i=$i"), source, 8, Seq("j", "k"))
          }
        }
      }
    }
  }
}
