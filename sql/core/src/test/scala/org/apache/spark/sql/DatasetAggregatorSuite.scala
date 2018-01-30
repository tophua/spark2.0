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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StringType


object ComplexResultAgg extends Aggregator[(String, Int), (Long, Long), (Long, Long)] {
  override def zero: (Long, Long) = (0, 0)
  override def reduce(countAndSum: (Long, Long), input: (String, Int)): (Long, Long) = {
    (countAndSum._1 + 1, countAndSum._2 + input._2)
  }
  override def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }
  override def finish(reduction: (Long, Long)): (Long, Long) = reduction
  override def bufferEncoder: Encoder[(Long, Long)] = Encoders.product[(Long, Long)]
  override def outputEncoder: Encoder[(Long, Long)] = Encoders.product[(Long, Long)]
}


case class AggData(a: Int, b: String)

object ClassInputAgg extends Aggregator[AggData, Int, Int] {
  override def zero: Int = 0
  override def reduce(b: Int, a: AggData): Int = b + a.a
  override def finish(reduction: Int): Int = reduction
  override def merge(b1: Int, b2: Int): Int = b1 + b2
  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt
  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}


object ClassBufferAggregator extends Aggregator[AggData, AggData, Int] {
  override def zero: AggData = AggData(0, "")
  override def reduce(b: AggData, a: AggData): AggData = AggData(b.a + a.a, "")
  override def finish(reduction: AggData): Int = reduction.a
  override def merge(b1: AggData, b2: AggData): AggData = AggData(b1.a + b2.a, "")
  override def bufferEncoder: Encoder[AggData] = Encoders.product[AggData]
  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}


object ComplexBufferAgg extends Aggregator[AggData, (Int, AggData), Int] {
  override def zero: (Int, AggData) = 0 -> AggData(0, "0")
  override def reduce(b: (Int, AggData), a: AggData): (Int, AggData) = (b._1 + 1, a)
  override def finish(reduction: (Int, AggData)): Int = reduction._1
  override def merge(b1: (Int, AggData), b2: (Int, AggData)): (Int, AggData) =
    (b1._1 + b2._1, b1._2)
  override def bufferEncoder: Encoder[(Int, AggData)] = Encoders.product[(Int, AggData)]
  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}


object MapTypeBufferAgg extends Aggregator[Int, Map[Int, Int], Int] {
  override def zero: Map[Int, Int] = Map.empty
  override def reduce(b: Map[Int, Int], a: Int): Map[Int, Int] = b
  override def finish(reduction: Map[Int, Int]): Int = 1
  override def merge(b1: Map[Int, Int], b2: Map[Int, Int]): Map[Int, Int] = b1
  override def bufferEncoder: Encoder[Map[Int, Int]] = ExpressionEncoder()
  override def outputEncoder: Encoder[Int] = ExpressionEncoder()
}


object NameAgg extends Aggregator[AggData, String, String] {
  def zero: String = ""
  def reduce(b: String, a: AggData): String = a.b + b
  def merge(b1: String, b2: String): String = b1 + b2
  def finish(r: String): String = r
  override def bufferEncoder: Encoder[String] = Encoders.STRING
  override def outputEncoder: Encoder[String] = Encoders.STRING
}


object SeqAgg extends Aggregator[AggData, Seq[Int], Seq[(Int, Int)]] {
  def zero: Seq[Int] = Nil
  def reduce(b: Seq[Int], a: AggData): Seq[Int] = a.a +: b
  def merge(b1: Seq[Int], b2: Seq[Int]): Seq[Int] = b1 ++ b2
  def finish(r: Seq[Int]): Seq[(Int, Int)] = r.map(i => i -> i)
  override def bufferEncoder: Encoder[Seq[Int]] = ExpressionEncoder()
  override def outputEncoder: Encoder[Seq[(Int, Int)]] = ExpressionEncoder()
}


class ParameterizedTypeSum[IN, OUT : Numeric : Encoder](f: IN => OUT)
  extends Aggregator[IN, OUT, OUT] {

  private val numeric = implicitly[Numeric[OUT]]
  override def zero: OUT = numeric.zero
  override def reduce(b: OUT, a: IN): OUT = numeric.plus(b, f(a))
  override def merge(b1: OUT, b2: OUT): OUT = numeric.plus(b1, b2)
  override def finish(reduction: OUT): OUT = reduction
  override def bufferEncoder: Encoder[OUT] = implicitly[Encoder[OUT]]
  override def outputEncoder: Encoder[OUT] = implicitly[Encoder[OUT]]
}

object RowAgg extends Aggregator[Row, Int, Int] {
  def zero: Int = 0
  def reduce(b: Int, a: Row): Int = a.getInt(0) + b
  def merge(b1: Int, b2: Int): Int = b1 + b2
  def finish(r: Int): Int = r
  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt
  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

object NullResultAgg extends Aggregator[AggData, AggData, AggData] {
  override def zero: AggData = AggData(0, "")
  override def reduce(b: AggData, a: AggData): AggData = AggData(b.a + a.a, b.b + a.b)
  override def finish(reduction: AggData): AggData = {
    if (reduction.a % 2 == 0) null else reduction
  }
  override def merge(b1: AggData, b2: AggData): AggData = AggData(b1.a + b2.a, b1.b + b2.b)
  override def bufferEncoder: Encoder[AggData] = Encoders.product[AggData]
  override def outputEncoder: Encoder[AggData] = Encoders.product[AggData]
}

case class ComplexAggData(d1: AggData, d2: AggData)

object VeryComplexResultAgg extends Aggregator[Row, String, ComplexAggData] {
  override def zero: String = ""
  override def reduce(buffer: String, input: Row): String = buffer + input.getString(1)
  override def merge(b1: String, b2: String): String = b1 + b2
  override def finish(reduction: String): ComplexAggData = {
    ComplexAggData(AggData(reduction.length, reduction), AggData(reduction.length, reduction))
  }
  override def bufferEncoder: Encoder[String] = Encoders.STRING
  override def outputEncoder: Encoder[ComplexAggData] = Encoders.product[ComplexAggData]
}


class DatasetAggregatorSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  private implicit val ordering = Ordering.by((c: AggData) => c.a -> c.b)
  //类型聚合:TypedAggregator
  test("typed aggregation: TypedAggregator") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDataset(
      ds.groupByKey(_._1).agg(typed.sum(_._2)),
      ("a", 30.0), ("b", 3.0), ("c", 1.0))
  }
  //类型聚合:TypedAggregator,expr,expr
  test("typed aggregation: TypedAggregator, expr, expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()

    checkDataset(
      ds.groupByKey(_._1).agg(
        typed.sum(_._2),
        expr("sum(_2)").as[Long],
        count("*")),
      ("a", 30.0, 30L, 2L), ("b", 3.0, 3L, 2L), ("c", 1.0, 1L, 1L))
  }
  //类型聚合:复杂的结果类型
  test("typed aggregation: complex result type") {
    val ds = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDS()

    checkDataset(
      ds.groupByKey(_._1).agg(
        expr("avg(_2)").as[Double],
        ComplexResultAgg.toColumn),
      ("a", 2.0, (2L, 4L)), ("b", 3.0, (1L, 3L)))
  }
  //键入聚合:在项目列
  test("typed aggregation: in project list") {
    val ds = Seq(1, 3, 2, 5).toDS()

    checkDataset(
      ds.select(typed.sum((i: Int) => i)),
      11.0)
    checkDataset(
      ds.select(typed.sum((i: Int) => i), typed.sum((i: Int) => i * 2)),
      11.0 -> 22.0)
  }
  //类型聚合:类输入
  test("typed aggregation: class input") {
    val ds = Seq(AggData(1, "one"), AggData(2, "two")).toDS()

    checkDataset(
      ds.select(ClassInputAgg.toColumn),
      3)
  }
  //键入聚合:类输入与重新排序
  test("typed aggregation: class input with reordering") {
    val ds = sql("SELECT 'one' AS b, 1 as a").as[AggData]

    checkDataset(
      ds.select(ClassInputAgg.toColumn),
      1)

    checkDataset(
      ds.select(expr("avg(a)").as[Double], ClassInputAgg.toColumn),
      (1.0, 1))

    checkDataset(
      ds.groupByKey(_.b).agg(ClassInputAgg.toColumn),
      ("one", 1))
  }
  //使用聚合器的类型聚合
  test("Typed aggregation using aggregator") {
    // based on Dataset complex Aggregator test of DatasetBenchmark
    //基于DatasetBasech集合的数据集合测试
    val ds = Seq(AggData(1, "x"), AggData(2, "y"), AggData(3, "z")).toDS()
    checkDataset(
      ds.select(ClassBufferAggregator.toColumn),
      6)
  }
  //类型聚合:复杂的输入
  test("typed aggregation: complex input") {
    val ds = Seq(AggData(1, "one"), AggData(2, "two")).toDS()

    checkDataset(
      ds.select(ComplexBufferAgg.toColumn),
      2
    )

    checkDataset(
      ds.select(expr("avg(a)").as[Double], ComplexBufferAgg.toColumn),
      (1.5, 2))

    checkDatasetUnorderly(
      ds.groupByKey(_.b).agg(ComplexBufferAgg.toColumn),
      ("one", 1), ("two", 1))
  }
  //类型集合:平均,计数,总和
  test("typed aggregate: avg, count, sum") {
    val ds = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDS()
    checkDataset(
      ds.groupByKey(_._1).agg(
        typed.avg(_._2), typed.count(_._2), typed.sum(_._2), typed.sumLong(_._2)),
      ("a", 2.0, 2L, 4.0, 4L), ("b", 3.0, 1L, 3.0, 3L))
  }
  //产生类型总和
  test("generic typed sum") {
    val ds = Seq("a" -> 1, "a" -> 3, "b" -> 3).toDS()
    checkDataset(
      ds.groupByKey(_._1)
        .agg(new ParameterizedTypeSum[(String, Int), Double](_._2.toDouble).toColumn),
      ("a", 4.0), ("b", 3.0))

    checkDataset(
      ds.groupByKey(_._1)
        .agg(new ParameterizedTypeSum((x: (String, Int)) => x._2.toInt).toColumn),
      ("a", 4), ("b", 3))
  }
  //输入列重新排序后,结果不应该被破坏
  test("SPARK-12555 - result should not be corrupted after input columns are reordered") {
    val ds = sql("SELECT 'Some String' AS b, 1279869254 AS a").as[AggData]

    checkDataset(
      ds.groupByKey(_.a).agg(NameAgg.toColumn),
        (1279869254, "Some String"))
  }
  //DataFrame/Dataset中的聚合器[Row]
  test("aggregator in DataFrame/Dataset[Row]") {
    val df = Seq(1 -> "a", 2 -> "b", 3 -> "b").toDF("i", "j")
    checkAnswer(df.groupBy($"j").agg(RowAgg.toColumn), Row("a", 1) :: Row("b", 5) :: Nil)
  }
  //使用Seq作为聚集缓冲区类型时,ClassFormatError
  test("SPARK-14675: ClassFormatError when use Seq as Aggregator buffer type") {
    val ds = Seq(AggData(1, "a"), AggData(2, "a")).toDS()

    checkDataset(
      ds.groupByKey(_.b).agg(SeqAgg.toColumn),
      "a" -> Seq(1 -> 1, 2 -> 2)
    )
  }
  //DataFrame/Dataset中的聚合器的别名[Row]
  test("spark-15051 alias of aggregator in DataFrame/Dataset[Row]") {
    val df1 = Seq(1 -> "a", 2 -> "b", 3 -> "b").toDF("i", "j")
    checkAnswer(df1.agg(RowAgg.toColumn as "b"), Row(6) :: Nil)

    val df2 = Seq(1 -> "a", 2 -> "b", 3 -> "b").toDF("i", "j")
    checkAnswer(df2.agg(RowAgg.toColumn as "b").select("b"), Row(6) :: Nil)
  }
  //较短的系统生成别名
  test("spark-15114 shorter system generated alias names") {
    val ds = Seq(1, 3, 2, 5).toDS()
    assert(ds.select(typed.sum((i: Int) => i)).columns.head === "TypedSumDouble(int)")
    val ds2 = ds.select(typed.sum((i: Int) => i), typed.avg((i: Int) => i))
    assert(ds2.columns.head === "TypedSumDouble(int)")
    assert(ds2.columns.last === "TypedAverage(int)")
    val df = Seq(1 -> "a", 2 -> "b", 3 -> "b").toDF("i", "j")
    assert(df.groupBy($"j").agg(RowAgg.toColumn).columns.last ==
      "RowAgg(org.apache.spark.sql.Row)")
    assert(df.groupBy($"j").agg(RowAgg.toColumn as "agg1").columns.last == "agg1")
  }
  //聚合器可以返回空结果
  test("SPARK-15814 Aggregator can return null result") {
    val ds = Seq(AggData(1, "one"), AggData(2, "two")).toDS()
    checkDatasetUnorderly(
      ds.groupByKey(_.a).agg(NullResultAgg.toColumn),
      1 -> AggData(1, "one"), 2 -> null)
  }
  //使用Map作为Aggregator的缓冲区类型
  test("SPARK-16100: use Map as the buffer type of Aggregator") {
    val ds = Seq(1, 2, 3).toDS()
    checkDataset(ds.select(MapTypeBufferAgg.toColumn), 1)
  }
  //提高聚合器的可推理性
  test("SPARK-15204 improve nullability inference for Aggregator") {
    val ds1 = Seq(1, 3, 2, 5).toDS()
    assert(ds1.select(typed.sum((i: Int) => i)).schema.head.nullable === false)
    val ds2 = Seq(AggData(1, "a"), AggData(2, "a")).toDS()
    assert(ds2.select(SeqAgg.toColumn).schema.head.nullable === true)
    val ds3 = sql("SELECT 'Some String' AS b, 1279869254 AS a").as[AggData]
    assert(ds3.select(NameAgg.toColumn).schema.head.nullable === true)
  }
  //非常复杂的聚合器结果类型
  test("SPARK-18147: very complex aggregator result type") {
    val df = Seq(1 -> "a", 2 -> "b", 2 -> "c").toDF("i", "j")

    checkAnswer(
      df.groupBy($"i").agg(VeryComplexResultAgg.toColumn),
      Row(1, Row(Row(1, "a"), Row(1, "a"))) :: Row(2, Row(Row(2, "bc"), Row(2, "bc"))) :: Nil)
  }
}