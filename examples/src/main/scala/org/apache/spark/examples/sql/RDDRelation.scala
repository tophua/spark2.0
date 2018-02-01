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

// scalastyle:off println
package org.apache.spark.examples.sql

import org.apache.spark.sql.SaveMode
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
//一种定义RDD模式的方法是用所需的列名和类型来创建一个case类
case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder
      .appName("Spark Examples")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    //导入SparkSession可以访问所有的SQL函数和隐式转换
    import spark.implicits._
    // $example off:init_session$

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.
    //任何包含案例类的RDD都可以用来创建临时视图,视图的模式是使用scala反射自动推断的
    df.createOrReplaceTempView("records")

    // Once tables have been registered, you can run SQL queries over them.
    //一旦表已经被注册,你可以运行SQL查询。
    println("Result of SELECT *:")
    spark.sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    //聚合查询也被支持
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions. The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    //SQL查询的结果本身就是RDD,并支持所有正常的RDD功能,RDD中的项目是Row类型,它允许您按顺序访问每个列。
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    //查询也可以使用类似LINQ的Scala DSL编写
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    // Write out an RDD as a parquet file with overwrite mode.
    //用覆写模式写一个RDD作为parquet文件
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    // Read in parquet file.  Parquet files are self-describing so the schema is preserved.
    //读parquet文件,parquet文件是自描述的,因此模式被保留。
    val parquetFile = spark.read.parquet("pair.parquet")

    // Queries can be run using the DSL on parquet files just like the original RDD.
    //查询可以像使用原始的RDD一样在parquet上使用DSL来运行。
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // These files can also be used to create a temporary view.
    //这些文件也可以用来创建一个临时视图
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)

    spark.stop()
  }
}
// scalastyle:on println
