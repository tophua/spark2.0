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

package org.apache.spark.sql.execution.joins

import scala.reflect.ClassTag

import org.apache.spark.AccumulatorSuite
import org.apache.spark.sql.{Dataset, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{BitwiseAnd, BitwiseOr, Cast, Literal, ShiftLeft}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{LongType, ShortType}

/**
 * Test various broadcast join operators.
 * 测试各种广播连接操作符
 * Tests in this suite we need to run Spark in local-cluster mode. In particular, the use of
 * unsafe map in [[org.apache.spark.sql.execution.joins.UnsafeHashedRelation]] is not triggered
 * without serializing the hashed relation, which does not happen in local mode.
 */
class BroadcastJoinSuite extends QueryTest with SQLTestUtils {
  import testImplicits._

  protected var spark: SparkSession = null

  /**
   * Create a new [[SparkSession]] running in local-cluster mode with unsafe and codegen enabled.
    * 创建一个新的[ ] [ ] sparksession运行在本地集群模式与不安全的代码生成功能
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("testing")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
    spark = null
  }

  /**
   * Test whether the specified broadcast join updates the peak execution memory accumulator.
    * 测试指定的广播连接是否更新峰值执行存储器累加器
   */
  private def testBroadcastJoinPeak[T: ClassTag](name: String, joinType: String): Unit = {
    AccumulatorSuite.verifyPeakExecutionMemorySet(spark.sparkContext, name) {
      val plan = testBroadcastJoin[T](joinType)
      plan.executeCollect()
    }
  }

  private def testBroadcastJoin[T: ClassTag](
      joinType: String,
      forceBroadcast: Boolean = false): SparkPlan = {
    val df1 = spark.createDataFrame(Seq((1, "4"), (2, "2"))).toDF("key", "value")
    val df2 = spark.createDataFrame(Seq((1, "1"), (2, "2"))).toDF("key", "value")

    // Comparison at the end is for broadcast left semi join
    //最后比较是广播左半连接
    val joinExpression = df1("key") === df2("key") && df1("value") > df2("value")
    val df3 = if (forceBroadcast) {
      df1.join(broadcast(df2), joinExpression, joinType)
    } else {
      df1.join(df2, joinExpression, joinType)
    }
    val plan = EnsureRequirements(spark.sessionState.conf).apply(df3.queryExecution.sparkPlan)
    assert(plan.collect { case p: T => p }.size === 1)
    plan
  }
  //不安全广播散列连接更新峰值执行内存
  test("unsafe broadcast hash join updates peak execution memory") {
    testBroadcastJoinPeak[BroadcastHashJoinExec]("unsafe broadcast hash join", "inner")
  }
  //不安全广播散列外部连接更新峰值执行内存
  test("unsafe broadcast hash outer join updates peak execution memory") {
    testBroadcastJoinPeak[BroadcastHashJoinExec]("unsafe broadcast hash outer join", "left_outer")
  }
  //不安全广播左半连接更新峰值执行内存
  test("unsafe broadcast left semi join updates peak execution memory") {
    testBroadcastJoinPeak[BroadcastHashJoinExec]("unsafe broadcast left semi join", "leftsemi")
  }
  //广播提示不authbroadcastjointhreshold设置为低值的困扰
  test("broadcast hint isn't bothered by authBroadcastJoinThreshold set to low values") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      testBroadcastJoin[BroadcastHashJoinExec]("inner", true)
    }
  }
  //广播提示不是由残疾人authbroadcastjointhreshold困扰
  test("broadcast hint isn't bothered by a disabled authBroadcastJoinThreshold") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      testBroadcastJoin[BroadcastHashJoinExec]("inner", true)
    }
  }
  //连接后不会传播广播提示
  test("broadcast hint isn't propagated after a join") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.createDataFrame(Seq((1, "4"), (2, "2"))).toDF("key", "value")
      val df2 = spark.createDataFrame(Seq((1, "1"), (2, "2"))).toDF("key", "value")
      val df3 = df1.join(broadcast(df2), Seq("key"), "inner").drop(df2("key"))

      val df4 = spark.createDataFrame(Seq((1, "5"), (2, "5"))).toDF("key", "value")
      val df5 = df4.join(df3, Seq("key"), "inner")

      val plan =
        EnsureRequirements(spark.sessionState.conf).apply(df5.queryExecution.sparkPlan)

      assert(plan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
      assert(plan.collect { case p: SortMergeJoinExec => p }.size === 1)
    }
  }

  private def assertBroadcastJoin(df : Dataset[Row]) : Unit = {
    val df1 = spark.createDataFrame(Seq((1, "4"), (2, "2"))).toDF("key", "value")
    val joined = df1.join(df, Seq("key"), "inner")

    val plan =
      EnsureRequirements(spark.sessionState.conf).apply(joined.queryExecution.sparkPlan)

    assert(plan.collect { case p: BroadcastHashJoinExec => p }.size === 1)
  }
  //广播提示编程API
  test("broadcast hint programming API") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df2 = spark.createDataFrame(Seq((1, "1"), (2, "2"), (3, "2"))).toDF("key", "value")
      val broadcasted = broadcast(df2)
      val df3 = spark.createDataFrame(Seq((2, "2"), (3, "3"))).toDF("key", "value")

      val cases = Seq(broadcasted.limit(2),
                      broadcasted.filter("value < 10"),
                      broadcasted.sample(true, 0.5),
                      broadcasted.distinct(),
                      broadcasted.groupBy("value").agg(min($"key").as("key")),
                      // except and intersect are semi/anti-joins which won't return more data then
                      // their left argument, so the broadcast hint should be propagated here
                      broadcasted.except(df3),
                      broadcasted.intersect(df3))

      cases.foreach(assertBroadcastJoin)
    }
  }
  //sql中的广播提示
  test("broadcast hint in SQL") {
    import org.apache.spark.sql.catalyst.plans.logical.{ResolvedHint, Join}

    spark.range(10).createOrReplaceTempView("t")
    spark.range(10).createOrReplaceTempView("u")

    for (name <- Seq("BROADCAST", "BROADCASTJOIN", "MAPJOIN")) {
      val plan1 = sql(s"SELECT /*+ $name(t) */ * FROM t JOIN u ON t.id = u.id").queryExecution
        .optimizedPlan
      val plan2 = sql(s"SELECT /*+ $name(u) */ * FROM t JOIN u ON t.id = u.id").queryExecution
        .optimizedPlan
      val plan3 = sql(s"SELECT /*+ $name(v) */ * FROM t JOIN u ON t.id = u.id").queryExecution
        .optimizedPlan

      assert(plan1.asInstanceOf[Join].left.isInstanceOf[ResolvedHint])
      assert(!plan1.asInstanceOf[Join].right.isInstanceOf[ResolvedHint])
      assert(!plan2.asInstanceOf[Join].left.isInstanceOf[ResolvedHint])
      assert(plan2.asInstanceOf[Join].right.isInstanceOf[ResolvedHint])
      assert(!plan3.asInstanceOf[Join].left.isInstanceOf[ResolvedHint])
      assert(!plan3.asInstanceOf[Join].right.isInstanceOf[ResolvedHint])
    }
  }
  //连接关键改写
  test("join key rewritten") {
    val l = Literal(1L)
    val i = Literal(2)
    val s = Literal.create(3, ShortType)
    val ss = Literal("hello")

    assert(HashJoin.rewriteKeyExpr(l :: Nil) === l :: Nil)
    assert(HashJoin.rewriteKeyExpr(l :: l :: Nil) === l :: l :: Nil)
    assert(HashJoin.rewriteKeyExpr(l :: i :: Nil) === l :: i :: Nil)

    assert(HashJoin.rewriteKeyExpr(i :: Nil) === Cast(i, LongType) :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: l :: Nil) === i :: l :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: i :: Nil) ===
      BitwiseOr(ShiftLeft(Cast(i, LongType), Literal(32)),
        BitwiseAnd(Cast(i, LongType), Literal((1L << 32) - 1))) :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: i :: i :: Nil) === i :: i :: i :: Nil)

    assert(HashJoin.rewriteKeyExpr(s :: Nil) === Cast(s, LongType) :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: l :: Nil) === s :: l :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: Nil) ===
      BitwiseOr(ShiftLeft(Cast(s, LongType), Literal(16)),
        BitwiseAnd(Cast(s, LongType), Literal((1L << 16) - 1))) :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: s :: Nil) ===
      BitwiseOr(ShiftLeft(
        BitwiseOr(ShiftLeft(Cast(s, LongType), Literal(16)),
          BitwiseAnd(Cast(s, LongType), Literal((1L << 16) - 1))),
        Literal(16)),
        BitwiseAnd(Cast(s, LongType), Literal((1L << 16) - 1))) :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: s :: s :: Nil) ===
      BitwiseOr(ShiftLeft(
        BitwiseOr(ShiftLeft(
          BitwiseOr(ShiftLeft(Cast(s, LongType), Literal(16)),
            BitwiseAnd(Cast(s, LongType), Literal((1L << 16) - 1))),
          Literal(16)),
          BitwiseAnd(Cast(s, LongType), Literal((1L << 16) - 1))),
        Literal(16)),
        BitwiseAnd(Cast(s, LongType), Literal((1L << 16) - 1))) :: Nil)
    assert(HashJoin.rewriteKeyExpr(s :: s :: s :: s :: s :: Nil) ===
      s :: s :: s :: s :: s :: Nil)

    assert(HashJoin.rewriteKeyExpr(ss :: Nil) === ss :: Nil)
    assert(HashJoin.rewriteKeyExpr(l :: ss :: Nil) === l :: ss :: Nil)
    assert(HashJoin.rewriteKeyExpr(i :: ss :: Nil) === i :: ss :: Nil)
  }
}
