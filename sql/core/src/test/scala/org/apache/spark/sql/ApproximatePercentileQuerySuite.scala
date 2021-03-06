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

import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.test.SharedSQLContext

/**
 * End-to-end tests for approximate percentile aggregate function.
  * 端到端的近似百分比聚合函数测试
 */
class ApproximatePercentileQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private val table = "percentile_test"

  test("percentile_approx, single percentile value") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""
             |SELECT
             |  percentile_approx(col, 0.25),
             |  percentile_approx(col, 0.5),
             |  percentile_approx(col, 0.75d),
             |  percentile_approx(col, 0.0),
             |  percentile_approx(col, 1.0),
             |  percentile_approx(col, 0),
             |  percentile_approx(col, 1)
             |FROM $table
           """.stripMargin),
        Row(250D, 500D, 750D, 1D, 1000D, 1D, 1000D)
      )
    }
  }
  //percentile_approx百分位值数组
  test("percentile_approx, array of percentile value") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  percentile_approx(col, array(0.25, 0.5, 0.75D)),
             |  count(col),
             |  percentile_approx(col, array(0.0, 1.0)),
             |  sum(col)
             |FROM $table
           """.stripMargin),
        Row(Seq(250D, 500D, 750D), 1000, Seq(1D, 1000D), 500500)
      )
    }
  }
  //percentile_approx，在分区的最小值的多个记录
  test("percentile_approx, multiple records with the minimum value in a partition") {
    withTempView(table) {
      spark.sparkContext.makeRDD(Seq(1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5), 4).toDF("col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT percentile_approx(col, array(0.5)) FROM $table"),
        Row(Seq(1.0D))
      )
    }
  }
  //百分位数，不同精度
  test("percentile_approx, with different accuracies") {

    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)

      // With different accuracies
      val expectedPercentile = 250D
      val accuracies = Array(1, 10, 100, 1000, 10000)
      val errors = accuracies.map { accuracy =>
        val df = spark.sql(s"SELECT percentile_approx(col, 0.25, $accuracy) FROM $table")
        val approximatePercentile = df.collect().head.getDouble(0)
        val error = Math.abs(approximatePercentile - expectedPercentile)
        error
      }

      // The larger accuracy value we use, the smaller error we get
      //们使用的精度值越大,得到的误差越小。
      assert(errors.sorted.sameElements(errors.reverse))
    }
  }
  //支持常数折叠的参数精度和百分比
  test("percentile_approx, supports constant folding for parameter accuracy and percentages") {
    withTempView(table) {
      (1 to 1000).toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT percentile_approx(col, array(0.25 + 0.25D), 200 + 800D) FROM $table"),
        Row(Seq(500D))
      )
    }
  }
  //在空输入表上聚集，没有按组
  test("percentile_approx(), aggregation on empty input table, no group by") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT sum(col), percentile_approx(col, 0.5) FROM $table"),
        Row(null, null)
      )
    }
  }
  //在空输入表上聚合，按组按
  test("percentile_approx(), aggregation on empty input table, with group by") {
    withTempView(table) {
      Seq.empty[Int].toDF("col").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(s"SELECT sum(col), percentile_approx(col, 0.5) FROM $table GROUP BY col"),
        Seq.empty[Row]
      )
    }
  }

  test("percentile_approx(null), aggregation with group by") {
    withTempView(table) {
      (1 to 1000).map(x => (x % 3, x)).toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
             |  key,
             |  percentile_approx(null, 0.5)
             |FROM $table
             |GROUP BY key
           """.stripMargin),
        Seq(
          Row(0, null),
          Row(1, null),
          Row(2, null))
      )
    }
  }

  test("percentile_approx(null), aggregation without group by") {
    withTempView(table) {
      (1 to 1000).map(x => (x % 3, x)).toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  percentile_approx(null, 0.5),
              |  sum(null),
              |  percentile_approx(null, 0.5)
              |FROM $table
           """.stripMargin),
         Row(null, null, null)
      )
    }
  }

  test("percentile_approx(col, ...), input rows contains null, with out group by") {
    withTempView(table) {
      (1 to 1000).map(new Integer(_)).flatMap(Seq(null: Integer, _)).toDF("col")
        .createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  percentile_approx(col, 0.5),
              |  sum(null),
              |  percentile_approx(col, 0.5)
              |FROM $table
           """.stripMargin),
        Row(500D, null, 500D))
    }
  }

  test("percentile_approx(col, ...), input rows contains null, with group by") {
    withTempView(table) {
      val rand = new java.util.Random()
      (1 to 1000)
        .map(new Integer(_))
        .map(v => (new Integer(v % 2), v))
        // Add some nulls
        .flatMap(Seq(_, (null: Integer, null: Integer)))
        .toDF("key", "value").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
              |  percentile_approx(value, 0.5),
              |  sum(value),
              |  percentile_approx(value, 0.5)
              |FROM $table
              |GROUP BY key
           """.stripMargin),
        Seq(
          Row(499.0D, 250000, 499.0D),
          Row(500.0D, 250500, 500.0D),
          Row(null, null, null))
      )
    }
  }

  test("percentile_approx(col, ...) works in window function") {
    withTempView(table) {
      val data = (1 to 10).map(v => (v % 2, v))
      data.toDF("key", "value").createOrReplaceTempView(table)

      val query = spark.sql(
        s"""
           |SElECT percentile_approx(value, 0.5)
           |OVER
           |  (PARTITION BY key ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
           |    AS percentile
           |FROM $table
           """.stripMargin)

      val expected = data.groupBy(_._1).toSeq.flatMap { group =>
        val (key, values) = group
        val sortedValues = values.map(_._2).sorted

        var outputRows = Seq.empty[Row]
        var i = 0

        val percentile = new PercentileDigest(1.0 / DEFAULT_PERCENTILE_ACCURACY)
        sortedValues.foreach { value =>
          percentile.add(value)
          outputRows :+= Row(percentile.getPercentiles(Array(0.5D)).head)
        }
        outputRows
      }

      checkAnswer(query, expected)
    }
  }
}
