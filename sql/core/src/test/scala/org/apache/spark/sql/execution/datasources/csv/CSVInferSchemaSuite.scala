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

package org.apache.spark.sql.execution.datasources.csv

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class CSVInferSchemaSuite extends SparkFunSuite {
  //字符串字段类型从null类型正确推断
  test("String fields types are inferred correctly from null types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(NullType, "", options) == NullType)
    assert(CSVInferSchema.inferField(NullType, null, options) == NullType)
    assert(CSVInferSchema.inferField(NullType, "100000000000", options) == LongType)
    assert(CSVInferSchema.inferField(NullType, "60", options) == IntegerType)
    assert(CSVInferSchema.inferField(NullType, "3.5", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(NullType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(NullType, "True", options) == BooleanType)
    assert(CSVInferSchema.inferField(NullType, "FAlSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(CSVInferSchema.inferField(NullType, textValueOne, options) == expectedTypeOne)
  }
  //字符串字段类型是从其他类型正确推断的
  test("String fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(LongType, "1.0", options) == DoubleType)
    assert(CSVInferSchema.inferField(LongType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(IntegerType, "1.0", options) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, null, options) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08-20 14:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(LongType, "True", options) == BooleanType)
    assert(CSVInferSchema.inferField(IntegerType, "FALSE", options) == BooleanType)
    assert(CSVInferSchema.inferField(TimestampType, "FALSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(CSVInferSchema.inferField(IntegerType, textValueOne, options) == expectedTypeOne)
  }
  //时间戳记字段类型通过自定义数据格式正确推断
  test("Timestamp field types are inferred correctly via custom data format") {
    var options = new CSVOptions(Map("timestampFormat" -> "yyyy-mm"), "GMT")
    assert(CSVInferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
    options = new CSVOptions(Map("timestampFormat" -> "yyyy"), "GMT")
    assert(CSVInferSchema.inferField(TimestampType, "2015", options) == TimestampType)
  }
  //时间戳记字段类型可以从其他类型正确地推断出来
  test("Timestamp field types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(IntegerType, "2015-08-20 14", options) == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 14:10", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08 14:49:00", options) == StringType)
  }
  //布尔字段类型是从其他类型正确推断的
  test("Boolean fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(LongType, "Fale", options) == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "TRUEe", options) == StringType)
  }
  //类型数组被合并到最高的通用类型
  test("Type arrays are merged to highest common type") {
    assert(
      CSVInferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }
  //指定nullValue时,正确处理空字段
  test("Null fields are handled properly when a nullValue is specified") {
    var options = new CSVOptions(Map("nullValue" -> "null"), "GMT")
    assert(CSVInferSchema.inferField(NullType, "null", options) == NullType)
    assert(CSVInferSchema.inferField(StringType, "null", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "null", options) == LongType)

    options = new CSVOptions(Map("nullValue" -> "\\N"), "GMT")
    assert(CSVInferSchema.inferField(IntegerType, "\\N", options) == IntegerType)
    assert(CSVInferSchema.inferField(DoubleType, "\\N", options) == DoubleType)
    assert(CSVInferSchema.inferField(TimestampType, "\\N", options) == TimestampType)
    assert(CSVInferSchema.inferField(BooleanType, "\\N", options) == BooleanType)
    assert(CSVInferSchema.inferField(DecimalType(1, 1), "\\N", options) == DecimalType(1, 1))
  }
  //合并Nulltypes应该产生Nulltype
  test("Merging Nulltypes should yield Nulltype.") {
    val mergedNullTypes = CSVInferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.deep == Array(NullType).deep)
  }
  //将DataSource选项键改为更不区分大小写
  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new CSVOptions(Map("TiMeStampFormat" -> "yyyy-mm"), "GMT")
    assert(CSVInferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
  }
  //DecimalType上的`inferField`应该找到`typeSoFar`的通用类型
  test("SPARK-18877: `inferField` on DecimalType should find a common type with `typeSoFar`") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")

    // 9.03E+12 is Decimal(3, -10) and 1.19E+11 is Decimal(3, -9).
    assert(CSVInferSchema.inferField(DecimalType(3, -10), "1.19E+11", options) ==
      DecimalType(4, -9))

    // BigDecimal("12345678901234567890.01234567890123456789") is precision 40 and scale 20.
    val value = "12345678901234567890.01234567890123456789"
    assert(CSVInferSchema.inferField(DecimalType(3, -10), value, options) == DoubleType)

    // Seq(s"${Long.MaxValue}1", "2015-12-01 00:00:00") should be StringType
    assert(CSVInferSchema.inferField(NullType, s"${Long.MaxValue}1", options) == DecimalType(20, 0))
    assert(CSVInferSchema.inferField(DecimalType(20, 0), "2015-12-01 00:00:00", options)
      == StringType)
  }
  //提供用户定义的nan / inf时，应该传递DoubleType
  test("DoubleType should be infered when user defined nan/inf are provided") {
    val options = new CSVOptions(Map("nanValue" -> "nan", "negativeInf" -> "-inf",
      "positiveInf" -> "inf"), "GMT")
    assert(CSVInferSchema.inferField(NullType, "nan", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "inf", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "-inf", options) == DoubleType)
  }
}
