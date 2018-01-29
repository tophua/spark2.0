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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * An end-to-end test suite specifically for testing Tungsten (Unsafe/CodeGen) mode.
  * 一个专门用于测试钨(不安全/CodeGen)模式的端到端测试套件
 *
 * This is here for now so I can make sure Tungsten project is tested without refactoring existing
 * end-to-end test infra. In the long run this should just go away.
  *这是现在这里,所以我可以确保钨项目进行测试,而不重构现有的端到端测试基础,从长远来看,这应该会消失。
 */
class DataFrameTungstenSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  //测试简单的类型
  test("test simple types") {
    val df = sparkContext.parallelize(Seq((1, 2))).toDF("a", "b")
    assert(df.select(struct("a", "b")).first().getStruct(0) === Row(1, 2))
  }
  //测试结构类型
  test("test struct type") {
    val struct = Row(1, 2L, 3.0F, 3.0)
    val data = sparkContext.parallelize(Seq(Row(1, struct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType))

    val df = spark.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(struct))
  }
  //测试嵌套的结构类型
  test("test nested struct type") {
    val innerStruct = Row(1, "abcd")
    val outerStruct = Row(1, 2L, 3.0F, 3.0, innerStruct, "efg")
    val data = sparkContext.parallelize(Seq(Row(1, outerStruct)))

    val schema = new StructType()
      .add("a", IntegerType)
      .add("b",
        new StructType()
          .add("b1", IntegerType)
          .add("b2", LongType)
          .add("b3", FloatType)
          .add("b4", DoubleType)
          .add("b5", new StructType()
          .add("b5a", IntegerType)
          .add("b5b", StringType))
          .add("b6", StringType))

    val df = spark.createDataFrame(data, schema)
    assert(df.select("b").first() === Row(outerStruct))
  }
}
