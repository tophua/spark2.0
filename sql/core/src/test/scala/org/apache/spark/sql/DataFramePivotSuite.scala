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

import org.apache.spark.sql.catalyst.expressions.aggregate.PivotFirst
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
  //枢轴
class DataFramePivotSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  //枢轴课程
  test("pivot courses") {
    checkAnswer(
      courseSales.groupBy("year").pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings")),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }
  //枢轴年
  test("pivot year") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq(2012, 2013)).agg(sum($"earnings")),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }
  //透过多个聚合枢纽课程
  test("pivot courses with multiple aggregations") {
    checkAnswer(
      courseSales.groupBy($"year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings"), avg($"earnings")),
      Row(2012, 15000.0, 7500.0, 20000.0, 20000.0) ::
        Row(2013, 48000.0, 48000.0, 30000.0, 30000.0) :: Nil
    )
  }
  //使用字符串值来转换年份（转换）
  test("pivot year with string values (cast)") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq("2012", "2013")).sum("earnings"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }
  //以int值为单位进行年份
  test("pivot year with int values") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year", Seq(2012, 2013)).sum("earnings"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }
  //透视课程没有价值
  test("pivot courses with no values") {
    // Note Java comes before dotNet in sorted order
    checkAnswer(
      courseSales.groupBy("year").pivot("course").agg(sum($"earnings")),
      Row(2012, 20000.0, 15000.0) :: Row(2013, 30000.0, 48000.0) :: Nil
    )
  }
  //数据透视年，没有值
  test("pivot year with no values") {
    checkAnswer(
      courseSales.groupBy("course").pivot("year").agg(sum($"earnings")),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }
  //pivot max values enforced
    //最大值强制执行
  test("pivot max values enforced") {
    spark.conf.set(SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key, 1)
    intercept[AnalysisException](
      courseSales.groupBy("year").pivot("course")
    )
    spark.conf.set(SQLConf.DATAFRAME_PIVOT_MAX_VALUES.key,
      SQLConf.DATAFRAME_PIVOT_MAX_VALUES.defaultValue.get)
  }
  //以未解决的功能为中心
  test("pivot with UnresolvedFunction") {
    checkAnswer(
      courseSales.groupBy("year").pivot("course", Seq("dotNET", "Java"))
        .agg("earnings" -> "sum"),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }

  // Tests for optimized pivot (with PivotFirst) below
  //优化的枢纽计划
  test("optimized pivot planned") {
    val df = courseSales.groupBy("year")
      // pivot with extra columns to trigger optimization
      //透过额外的列来触发优化
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
    val queryExecution = spark.sessionState.executePlan(df.queryExecution.logical)
    assert(queryExecution.simpleString.contains("pivotfirst"))
  }

  //优化文字的枢轴课程
  test("optimized pivot courses with literals") {
    checkAnswer(
      courseSales.groupBy("year")
        // pivot with extra columns to trigger optimization
        //透过额外的列来触发优化
        .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
        .agg(sum($"earnings"))
        .select("year", "dotNET", "Java"),
      Row(2012, 15000.0, 20000.0) :: Row(2013, 48000.0, 30000.0) :: Nil
    )
  }
  //优化文字的年份
  test("optimized pivot year with literals") {
    checkAnswer(
      courseSales.groupBy($"course")
        // pivot with extra columns to trigger optimization
        //透过额外的列来触发优化
        .pivot("year", Seq(2012, 2013) ++ (1 to 10))
        .agg(sum($"earnings"))
        .select("course", "2012", "2013"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }
  //带字符串值的优化数据透视表（转换）
  test("optimized pivot year with string values (cast)") {
    checkAnswer(
      courseSales.groupBy("course")
        // pivot with extra columns to trigger optimization
        //透过额外的列来触发优化
        .pivot("year", Seq("2012", "2013") ++ (1 to 10).map(_.toString))
        .sum("earnings")
        .select("course", "2012", "2013"),
      Row("dotNET", 15000.0, 48000.0) :: Row("Java", 20000.0, 30000.0) :: Nil
    )
  }
  //优化枢轴DecimalType
  test("optimized pivot DecimalType") {
    val df = courseSales.select($"course", $"year", $"earnings".cast(DecimalType(10, 2)))
      .groupBy("year")
      // pivot with extra columns to trigger optimization
      //透过额外的列来触发优化
      .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
      .agg(sum($"earnings"))
      .select("year", "dotNET", "Java")

    assertResult(IntegerType)(df.schema("year").dataType)
    assertResult(DecimalType(20, 2))(df.schema("Java").dataType)
    assertResult(DecimalType(20, 2))(df.schema("dotNET").dataType)

    checkAnswer(df, Row(2012, BigDecimal(1500000, 2), BigDecimal(2000000, 2)) ::
      Row(2013, BigDecimal(4800000, 2), BigDecimal(3000000, 2)) :: Nil)
  }
  //透视第一个支持的数据类型
  test("PivotFirst supported datatypes") {
    val supportedDataTypes: Seq[DataType] = DoubleType :: IntegerType :: LongType :: FloatType ::
      BooleanType :: ShortType :: ByteType :: Nil
    for (datatype <- supportedDataTypes) {
      assertResult(true)(PivotFirst.supportsDataType(datatype))
    }
    assertResult(true)(PivotFirst.supportsDataType(DecimalType(10, 1)))
    assertResult(false)(PivotFirst.supportsDataType(null))
    assertResult(false)(PivotFirst.supportsDataType(ArrayType(IntegerType)))
  }
  //多个聚合优化枢轴
  test("optimized pivot with multiple aggregations") {
    checkAnswer(
      courseSales.groupBy($"year")
        // pivot with extra columns to trigger optimization
        //透过额外的列来触发优化
        .pivot("course", Seq("dotNET", "Java") ++ (1 to 10).map(_.toString))
        .agg(sum($"earnings"), avg($"earnings")),
      Row(Seq(2012, 15000.0, 7500.0, 20000.0, 20000.0) ++ Seq.fill(20)(null): _*) ::
        Row(Seq(2013, 48000.0, 48000.0, 30000.0, 30000.0) ++ Seq.fill(20)(null): _*) :: Nil
    )
  }
  //以PivotFirst不支持的数据类型为中心
  test("pivot with datatype not supported by PivotFirst") {
    checkAnswer(
      complexData.groupBy().pivot("b", Seq(true, false)).agg(max("a")),
      Row(Seq(1, 1, 1), Seq(2, 2, 2)) :: Nil
    )
  }
  //以PivotFirst 2不支持的数据类型为中心
  test("pivot with datatype not supported by PivotFirst 2") {
    checkAnswer(
      courseSales.withColumn("e", expr("array(earnings, 7.0d)"))
        .groupBy("year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(min($"e")),
      Row(2012, Seq(5000.0, 7.0), Seq(20000.0, 7.0)) ::
        Row(2013, Seq(48000.0, 7.0), Seq(30000.0, 7.0)) :: Nil
    )
  }
  //枢轴保留别名,如果给出
  test("pivot preserves aliases if given") {
    assertResult(
      Array("year", "dotNET_foo", "dotNET_avg(earnings)", "Java_foo", "Java_avg(earnings)")
    )(
      courseSales.groupBy($"year")
        .pivot("course", Seq("dotNET", "Java"))
        .agg(sum($"earnings").as("foo"), avg($"earnings")).columns
    )
  }
  //在groupby中定义列定义
  test("pivot with column definition in groupby") {
    checkAnswer(
      courseSales.groupBy(substring(col("course"), 0, 1).as("foo"))
        .pivot("year", Seq(2012, 2013))
        .sum("earnings"),
      Row("d", 15000.0, 48000.0) :: Row("J", 20000.0, 30000.0) :: Nil
    )
  }
  //使用null的枢轴不应该抛出NPE
  test("pivot with null should not throw NPE") {
    checkAnswer(
      Seq(Tuple1(None), Tuple1(Some(1))).toDF("a").groupBy($"a").pivot("a").count(),
      Row(null, 1, null) :: Row(1, null, 1) :: Nil)
  }
  //以null和PivotFirst不支持的聚合类型为中心返回正确的结果
  test("pivot with null and aggregate type not supported by PivotFirst returns correct result") {
    checkAnswer(
      Seq(Tuple1(None), Tuple1(Some(1))).toDF("a")
        .withColumn("b", expr("array(a, 7)"))
        .groupBy($"a").pivot("a").agg(min($"b")),
      Row(null, Seq(null, 7), null) :: Row(1, null, Seq(1, 7)) :: Nil)
  }
  //与时间戳和计数不应该打印内部表示
  test("pivot with timestamp and count should not print internal representation") {
    val ts = "2012-12-31 16:00:10.011"
    val tsWithZone = "2013-01-01 00:00:10.011"

    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "GMT") {
      val df = Seq(java.sql.Timestamp.valueOf(ts)).toDF("a").groupBy("a").pivot("a").count()
      val expected = StructType(
        StructField("a", TimestampType) ::
        StructField(tsWithZone, LongType) :: Nil)
      assert(df.schema == expected)
      // String representation of timestamp with timezone should take the time difference
      // into account.
      checkAnswer(df.select($"a".cast(StringType)), Row(tsWithZone))
    }
  }
}
