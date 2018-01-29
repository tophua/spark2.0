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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType}

class ExistenceJoinSuite extends SparkPlanTest with SharedSQLContext {

  private lazy val left = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0),
      Row(1, 2.0),
      Row(2, 1.0),
      Row(2, 1.0),
      Row(3, 3.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("a", IntegerType).add("b", DoubleType))

  private lazy val right = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(2, 3.0),
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val rightUniqueKey = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val singleConditionEQ = (left.col("a") === right.col("c")).expr

  private lazy val composedConditionEQ = {
    And((left.col("a") === right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  private lazy val composedConditionNEQ = {
    And((left.col("a") < right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testExistenceJoin(
      testName: String,
      joinType: JoinType,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      condition: => Expression,
      expectedAnswer: Seq[Row]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(condition))
      ExtractEquiJoinKeys.unapply(join)
    }

    val existsAttr = AttributeReference("exists", BooleanType, false)()
    val leftSemiPlus = ExistenceJoin(existsAttr)
    def createLeftSemiPlusJoin(join: SparkPlan): SparkPlan = {
      val output = join.output.dropRight(1)
      val condition = if (joinType == LeftSemi) {
        existsAttr
      } else {
        Not(existsAttr)
      }
      ProjectExec(output, FilterExec(condition, join))
    }

    test(s"$testName using ShuffledHashJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext.sessionState.conf).apply(
              ShuffledHashJoinExec(
                leftKeys, rightKeys, joinType, BuildRight, boundCondition, left, right)),
            expectedAnswer,
            sortAnswers = true)
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext.sessionState.conf).apply(
              createLeftSemiPlusJoin(ShuffledHashJoinExec(
                leftKeys, rightKeys, leftSemiPlus, BuildRight, boundCondition, left, right))),
            expectedAnswer,
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastHashJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext.sessionState.conf).apply(
              BroadcastHashJoinExec(
                leftKeys, rightKeys, joinType, BuildRight, boundCondition, left, right)),
            expectedAnswer,
            sortAnswers = true)
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext.sessionState.conf).apply(
              createLeftSemiPlusJoin(BroadcastHashJoinExec(
                leftKeys, rightKeys, leftSemiPlus, BuildRight, boundCondition, left, right))),
            expectedAnswer,
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using SortMergeJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext.sessionState.conf).apply(
              SortMergeJoinExec(leftKeys, rightKeys, joinType, boundCondition, left, right)),
            expectedAnswer,
            sortAnswers = true)
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext.sessionState.conf).apply(
              createLeftSemiPlusJoin(SortMergeJoinExec(
                leftKeys, rightKeys, leftSemiPlus, boundCondition, left, right))),
            expectedAnswer,
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build left") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          EnsureRequirements(left.sqlContext.sessionState.conf).apply(
            BroadcastNestedLoopJoinExec(left, right, BuildLeft, joinType, Some(condition))),
          expectedAnswer,
          sortAnswers = true)
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          EnsureRequirements(left.sqlContext.sessionState.conf).apply(
            createLeftSemiPlusJoin(BroadcastNestedLoopJoinExec(
              left, right, BuildLeft, leftSemiPlus, Some(condition)))),
          expectedAnswer,
          sortAnswers = true)
      }
    }

    test(s"$testName using BroadcastNestedLoopJoin build right") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          EnsureRequirements(left.sqlContext.sessionState.conf).apply(
            BroadcastNestedLoopJoinExec(left, right, BuildRight, joinType, Some(condition))),
          expectedAnswer,
          sortAnswers = true)
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          EnsureRequirements(left.sqlContext.sessionState.conf).apply(
            createLeftSemiPlusJoin(BroadcastNestedLoopJoinExec(
              left, right, BuildRight, leftSemiPlus, Some(condition)))),
          expectedAnswer,
          sortAnswers = true)
      }
    }
  }
  //测试左半连接的单条件（相等）
  testExistenceJoin(
    "test single condition (equal) for left semi join",
    LeftSemi,
    left,
    right,
    singleConditionEQ,
    Seq(Row(2, 1.0), Row(2, 1.0), Row(3, 3.0), Row(6, null)))
  //左半连接的测试组合条件（等于和不相等）
  testExistenceJoin(
    "test composed condition (equal & non-equal) for left semi join",
    LeftSemi,
    left,
    right,
    composedConditionEQ,
    Seq(Row(2, 1.0), Row(2, 1.0)))
  //左半连接的测试组合条件（非相等）
  testExistenceJoin(
    "test composed condition (both non-equal) for left semi join",
    LeftSemi,
    left,
    right,
    composedConditionNEQ,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(2, 1.0), Row(2, 1.0)))
  //测试左反连接的单条件（相等）
  testExistenceJoin(
    "test single condition (equal) for left Anti join",
    LeftAnti,
    left,
    right,
    singleConditionEQ,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(null, null), Row(null, 5.0)))

  //测试左反连接的唯一唯一条件（相等）
  testExistenceJoin(
    "test single unique condition (equal) for left Anti join",
    LeftAnti,
    left,
    right.select(right.col("c")).distinct(), /* Trigger BHJs unique key code path! */
    singleConditionEQ,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(null, null), Row(null, 5.0)))
  //反连接的测试组合条件（相等和非相等）检验
  testExistenceJoin(
    "test composed condition (equal & non-equal) test for anti join",
    LeftAnti,
    left,
    right,
    composedConditionEQ,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(3, 3.0), Row(6, null), Row(null, 5.0), Row(null, null)))
  //反连接的测试组合条件（非相等）
  testExistenceJoin(
    "test composed condition (both non-equal) for anti join",
    LeftAnti,
    left,
    right,
    composedConditionNEQ,
    Seq(Row(3, 3.0), Row(6, null), Row(null, 5.0), Row(null, null)))
  //测试反汇编的唯一条件（非相等）
  testExistenceJoin(
    "test composed unique condition (both non-equal) for anti join",
    LeftAnti,
    left,
    rightUniqueKey,
    (left.col("a") === rightUniqueKey.col("c") && left.col("b") < rightUniqueKey.col("d")).expr,
    Seq(Row(1, 2.0), Row(1, 2.0), Row(3, 3.0), Row(null, null), Row(null, 5.0), Row(6, null)))
}
