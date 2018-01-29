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

package org.apache.spark.sql.execution

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{MapOutputStatistics, SparkConf, SparkFunSuite}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.exchange.{ExchangeCoordinator, ShuffleExchange}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class ExchangeCoordinatorSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var originalActiveSparkSession: Option[SparkSession] = _
  private var originalInstantiatedSparkSession: Option[SparkSession] = _

  override protected def beforeAll(): Unit = {
    originalActiveSparkSession = SparkSession.getActiveSession
    originalInstantiatedSparkSession = SparkSession.getDefaultSession

    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    // Set these states back. 设置这些状态
    originalActiveSparkSession.foreach(ctx => SparkSession.setActiveSession(ctx))
    originalInstantiatedSparkSession.foreach(ctx => SparkSession.setDefaultSession(ctx))
  }

  private def checkEstimation(
      coordinator: ExchangeCoordinator,
      bytesByPartitionIdArray: Array[Array[Long]],
      expectedPartitionStartIndices: Array[Int]): Unit = {
    val mapOutputStatistics = bytesByPartitionIdArray.zipWithIndex.map {
      case (bytesByPartitionId, index) =>
        new MapOutputStatistics(index, bytesByPartitionId)
    }
    val estimatedPartitionStartIndices =
      coordinator.estimatePartitionStartIndices(mapOutputStatistics)
    assert(estimatedPartitionStartIndices === expectedPartitionStartIndices)
  }
  //测试estimatePartitionStartIndices - 1交换
  test("test estimatePartitionStartIndices - 1 Exchange") {
    val coordinator = new ExchangeCoordinator(1, 100L)

    {
      // All bytes per partition are 0.
      //每个分区的所有字节都是0
      val bytesByPartitionId = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // Some bytes per partition are 0 and total size is less than the target size.
      //每个分区的一些字节是0，总大小小于目标大小。
      // 1 post-shuffle partition is needed.
      //需要1个后洗牌分区
      val bytesByPartitionId = Array[Long](10, 0, 20, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partitions are needed.
      //需要2个后置洗牌分区
      val bytesByPartitionId = Array[Long](10, 0, 90, 20, 0)
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // There are a few large pre-shuffle partitions.
      //有几个大的预洗车分区
      val bytesByPartitionId = Array[Long](110, 10, 100, 110, 0)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // All pre-shuffle partitions are larger than the targeted size.
      //所有预混洗分区都大于目标大小
      val bytesByPartitionId = Array[Long](100, 110, 100, 110, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }

    {
      // The last pre-shuffle partition is in a single post-shuffle partition.
      //最后一个预洗牌分区在一个单独的后洗牌分区中
      val bytesByPartitionId = Array[Long](30, 30, 0, 40, 110)
      val expectedPartitionStartIndices = Array[Int](0, 4)
      checkEstimation(coordinator, Array(bytesByPartitionId), expectedPartitionStartIndices)
    }
  }
  //测试评估分区开始指数 - 2次交换
  test("test estimatePartitionStartIndices - 2 Exchanges") {
    val coordinator = new ExchangeCoordinator(2, 100L)

    {
      // If there are multiple values of the number of pre-shuffle partitions,
      //如果预混洗分区的数量有多个值，
      // we should see an assertion error.
      //我们应该看到一个断言错误
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0, 0)
      val mapOutputStatistics =
        Array(
          new MapOutputStatistics(0, bytesByPartitionId1),
          new MapOutputStatistics(1, bytesByPartitionId2))
      intercept[AssertionError](coordinator.estimatePartitionStartIndices(mapOutputStatistics))
    }

    {
      // All bytes per partition are 0.
      //每个分区的所有字节都是0
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // Some bytes per partition are 0.
      // 1 post-shuffle partition is needed.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 20, 0, 20)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partition are needed.
      val bytesByPartitionId1 = Array[Long](0, 10, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 4 post-shuffle partition are needed.
      //4后洗牌分区是必要的
      val bytesByPartitionId1 = Array[Long](0, 99, 0, 20, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // 2 post-shuffle partition are needed.
      //需要2个后洗牌分区
      val bytesByPartitionId1 = Array[Long](0, 100, 0, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 70, 0, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // There are a few large pre-shuffle partitions.
      //有几个大的预洗车分区
      val bytesByPartitionId1 = Array[Long](0, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 0, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // All pairs of pre-shuffle partitions are larger than the targeted size.
      //所有的预混洗分区对都大于目标尺寸
      val bytesByPartitionId1 = Array[Long](100, 100, 40, 30, 0)
      val bytesByPartitionId2 = Array[Long](30, 0, 60, 70, 110)
      val expectedPartitionStartIndices = Array[Int](0, 1, 2, 3, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }
  }
  //测试评估Partition StartIndices并执行最小数量的reducer
  test("test estimatePartitionStartIndices and enforce minimal number of reducers") {
    val coordinator = new ExchangeCoordinator(2, 100L, Some(2))

    {
      // The minimal number of post-shuffle partitions is not enforced because
      //由于数据的大小为0，因此不会强制实施后置式分区的最小数目。
      // the size of data is 0.
      val bytesByPartitionId1 = Array[Long](0, 0, 0, 0, 0)
      val bytesByPartitionId2 = Array[Long](0, 0, 0, 0, 0)
      val expectedPartitionStartIndices = Array[Int](0)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // The minimal number of post-shuffle partitions is enforced.
      //后置洗牌分区的最小数目被强制执行
      val bytesByPartitionId1 = Array[Long](10, 5, 5, 0, 20)
      val bytesByPartitionId2 = Array[Long](5, 10, 0, 10, 5)
      val expectedPartitionStartIndices = Array[Int](0, 3)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }

    {
      // The number of post-shuffle partitions is determined by the coordinator.
      //协调员确定后洗牌分区的数量
      val bytesByPartitionId1 = Array[Long](10, 50, 20, 80, 20)
      val bytesByPartitionId2 = Array[Long](40, 10, 0, 10, 30)
      val expectedPartitionStartIndices = Array[Int](0, 1, 3, 4)
      checkEstimation(
        coordinator,
        Array(bytesByPartitionId1, bytesByPartitionId2),
        expectedPartitionStartIndices)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Query tests
  ///////////////////////////////////////////////////////////////////////////

  val numInputPartitions: Int = 10

  def checkAnswer(actual: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(actual, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  def withSparkSession(
      f: SparkSession => Unit,
      targetNumPostShufflePartitions: Int,
      minNumPostShufflePartitions: Option[Int]): Unit = {
    val sparkConf =
      new SparkConf(false)
        .setMaster("local[*]")
        .setAppName("test")
        .set("spark.ui.enabled", "false")
        .set("spark.driver.allowMultipleContexts", "true")
        .set(SQLConf.SHUFFLE_PARTITIONS.key, "5")
        .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
        .set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        .set(
          SQLConf.SHUFFLE_TARGET_POSTSHUFFLE_INPUT_SIZE.key,
          targetNumPostShufflePartitions.toString)
    minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        sparkConf.set(SQLConf.SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS.key, numPartitions.toString)
      case None =>
        sparkConf.set(SQLConf.SHUFFLE_MIN_NUM_POSTSHUFFLE_PARTITIONS.key, "-1")
    }

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    try f(spark) finally spark.stop()
  }

  Seq(Some(5), None).foreach { minNumPostShufflePartitions =>
    val testNameNote = minNumPostShufflePartitions match {
      case Some(numPartitions) => "(minNumPostShufflePartitions: " + numPartitions + ")"
      case None => ""
    }
  //确定还原器的数量：集合运算符
    test(s"determining the number of reducers: aggregate operator$testNameNote") {
      val test = { spark: SparkSession =>
        val df =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 20 as key", "id as value")
        val agg = df.groupBy("key").count()

        // Check the answer first.
        checkAnswer(
          agg,
          spark.range(0, 20).selectExpr("id", "50 as cnt").collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = agg.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 1)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 3)
              case o =>
            }
        }
      }

      withSparkSession(test, 2000, minNumPostShufflePartitions)
    }
    //确定reducer的数量：join operator
    test(s"determining the number of reducers: join operator$testNameNote") {
      val test = { spark: SparkSession =>
        val df1 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
        val df2 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")

        val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("value2"))

        // Check the answer first.
        val expectedAnswer =
          spark
            .range(0, 1000)
            .selectExpr("id % 500 as key", "id as value")
            .union(spark.range(0, 1000).selectExpr("id % 500 as key", "id as value"))
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 2)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 2)
              case o =>
            }
        }
      }

      withSparkSession(test, 16384, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 1$testNameNote") {
      val test: (SparkSession) => Unit = { spark: SparkSession =>
        val df1 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
            .groupBy("key1")
            .count()
            .toDF("key1", "cnt1")
        val df2 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")
            .groupBy("key2")
            .count()
            .toDF("key2", "cnt2")

        val join = df1.join(df2, col("key1") === col("key2")).select(col("key1"), col("cnt2"))

        // Check the answer first.
        val expectedAnswer =
          spark
            .range(0, 500)
            .selectExpr("id", "2 as cnt")
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 4)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            assert(exchanges.forall(_.coordinator.isDefined))
            assert(exchanges.map(_.outputPartitioning.numPartitions).toSet === Set(2, 3))
        }
      }

      withSparkSession(test, 6644, minNumPostShufflePartitions)
    }

    test(s"determining the number of reducers: complex query 2$testNameNote") {
      val test: (SparkSession) => Unit = { spark: SparkSession =>
        val df1 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key1", "id as value1")
            .groupBy("key1")
            .count()
            .toDF("key1", "cnt1")
        val df2 =
          spark
            .range(0, 1000, 1, numInputPartitions)
            .selectExpr("id % 500 as key2", "id as value2")

        val join =
          df1
            .join(df2, col("key1") === col("key2"))
            .select(col("key1"), col("cnt1"), col("value2"))

        // Check the answer first.
        val expectedAnswer =
          spark
            .range(0, 1000)
            .selectExpr("id % 500 as key", "2 as cnt", "id as value")
        checkAnswer(
          join,
          expectedAnswer.collect())

        // Then, let's look at the number of post-shuffle partitions estimated
        // by the ExchangeCoordinator.
        val exchanges = join.queryExecution.executedPlan.collect {
          case e: ShuffleExchange => e
        }
        assert(exchanges.length === 3)
        minNumPostShufflePartitions match {
          case Some(numPartitions) =>
            exchanges.foreach {
              case e: ShuffleExchange =>
                assert(e.coordinator.isDefined)
                assert(e.outputPartitioning.numPartitions === 5)
              case o =>
            }

          case None =>
            assert(exchanges.forall(_.coordinator.isDefined))
            assert(exchanges.map(_.outputPartitioning.numPartitions).toSet === Set(5, 3))
        }
      }

      withSparkSession(test, 6144, minNumPostShufflePartitions)
    }
  }
}
