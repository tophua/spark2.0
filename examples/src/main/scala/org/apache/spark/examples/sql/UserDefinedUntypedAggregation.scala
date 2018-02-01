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
package org.apache.spark.examples.sql

// $example on:untyped_custom_aggregation$
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
// $example off:untyped_custom_aggregation$

object UserDefinedUntypedAggregation {

  // $example on:untyped_custom_aggregation$
  object MyAverage extends UserDefinedAggregateFunction {
    // Data types of input arguments of this aggregate function
    //这个聚集函数的输入参数的数据类型
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
    // Data types of values in the aggregation buffer
    //聚合缓冲区中值的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }
    // The data type of the returned value
    //返回值的数据类型
    def dataType: DataType = DoubleType
    // Whether this function always returns the same output on the identical input
    //此功能是否始终在相同的输入上返回相同的输出
    def deterministic: Boolean = true
    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }
    // Updates the given aggregation buffer `buffer` with new input data from `input`
    //用`input`中的新输入数据更新给定的聚合缓冲区`buffer'
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }
    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    //合并两个聚合缓冲区,并将更新的缓冲区值存回“缓冲区1”
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    // Calculates the final result 计算最终结果
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }
  // $example off:untyped_custom_aggregation$

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL user-defined DataFrames aggregation example")
      .getOrCreate()

    // $example on:untyped_custom_aggregation$
    // Register the function to access it
    //注册该功能来访问它
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("examples/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    // $example off:untyped_custom_aggregation$

    spark.stop()
  }

}
