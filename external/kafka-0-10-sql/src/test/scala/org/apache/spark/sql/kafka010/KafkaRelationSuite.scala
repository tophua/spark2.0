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

package org.apache.spark.sql.kafka010

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils
//Kafka关系套件
class KafkaRelationSuite extends QueryTest with BeforeAndAfter with SharedSQLContext {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  private var testUtils: KafkaTestUtils = _

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => new TopicPartition(topic, p)))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  private def createDF(
      topic: String,
      withOptions: Map[String, String] = Map.empty[String, String],
      brokerAddress: Option[String] = None) = {
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers",
        brokerAddress.getOrElse(testUtils.brokerAddress))
      .option("subscribe", topic)
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().selectExpr("CAST(value AS STRING)")
  }

  //显式最早到最新的偏移量
  test("explicit earliest to latest offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    //指定最早和最新的偏移值
    val df = createDF(topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // "latest" should late bind to the current (latest) offset in the df
    //“最新”应该延迟到DF当前(最新的)偏移
    testUtils.sendMessages(topic, (21 to 29).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }
  //默认开始和结束偏移
  test("default starting and ending offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Implicit offset values, should default to earliest and latest
    //隐式偏移值,应默认为最早和最迟
    val df = createDF(topic)
    // Test that we default to "earliest" and "latest"
    //测试我们默认为“最早”和“最新”
    checkAnswer(df, (0 to 20).map(_.toString).toDF)
  }
  //显式偏移
  test("explicit offsets") {
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 3)
    testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(topic, Array("20"), Some(2))

    // Test explicitly specified offsets
    //显式指定偏移测试
    val startPartitionOffsets = Map(
      new TopicPartition(topic, 0) -> -2L, // -2 => earliest
      new TopicPartition(topic, 1) -> -2L,
      new TopicPartition(topic, 2) -> 0L   // explicit earliest
    )
    val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)

    val endPartitionOffsets = Map(
      new TopicPartition(topic, 0) -> -1L, // -1 => latest
      new TopicPartition(topic, 1) -> -1L,
      //显式偏移量=最新
      new TopicPartition(topic, 2) -> 1L  // explicit offset happens to = the latest
    )
    val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)
    val df = createDF(topic,
        withOptions = Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // static offset partition 2, nothing should change
    //静态偏移分区2，什么都不应该改变
    testUtils.sendMessages(topic, (31 to 39).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // latest offset partition 1, should change
    //最新的偏移分区1，应该改变
    testUtils.sendMessages(topic, (21 to 30).map(_.toString).toArray, Some(1))
    checkAnswer(df, (0 to 30).map(_.toString).toDF)
  }
  //同dataframe复用查询
  test("reuse same dataframe in query") {
    // This test ensures that we do not cache the Kafka Consumer in KafkaRelation
    //这个测试以确保我们不会缓存在KafkaRelation的卡夫卡消费者
    val topic = newTopic()
    testUtils.createTopic(topic, partitions = 1)
    testUtils.sendMessages(topic, (0 to 10).map(_.toString).toArray, Some(0))

    // Specify explicit earliest and latest offset values
    //指定最早和最新的偏移值。
    val df = createDF(topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }
  //测试延迟绑定开始偏移
  test("test late binding start offsets") {
    // Kafka fails to remove the logs on Windows. See KAFKA-1194.
    //Kafka未能删除Windows上的日志。看到kafka-1194
    assume(!Utils.isWindows)

    var kafkaUtils: KafkaTestUtils = null
    try {
      /**
       * The following settings will ensure that all log entries
       * are removed following a call to cleanupLogs
        * 以下设置将确保所有日志记录都删除后,调用cleanuplogs
       */
      val brokerProps = Map[String, Object](
        "log.retention.bytes" -> 1.asInstanceOf[AnyRef], // retain nothing
        "log.retention.ms" -> 1.asInstanceOf[AnyRef]     // no wait time
      )
      kafkaUtils = new KafkaTestUtils(withBrokerProps = brokerProps)
      kafkaUtils.setup()

      val topic = newTopic()
      kafkaUtils.createTopic(topic, partitions = 1)
      kafkaUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
      // Specify explicit earliest and latest offset values
      //指定最早和最新的偏移值
      val df = createDF(topic,
        withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"),
        Some(kafkaUtils.brokerAddress))
      checkAnswer(df, (0 to 9).map(_.toString).toDF)
      // Blow away current set of messages.
      //吹掉当前的一组消息
      kafkaUtils.cleanupLogs()
      // Add some more data, but do not call cleanup
      //添加更多的数据,但不要调用清理
      kafkaUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(0))
      // Ensure that we late bind to the new starting position
      //确保我们迟到了新的起点
      checkAnswer(df, (10 to 19).map(_.toString).toDF)
    } finally {
      if (kafkaUtils != null) {
        kafkaUtils.teardown()
      }
    }
  }
  //错误的批量查询选项
  test("bad batch query options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark
          .read
          .format("kafka")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    // Specifying an ending offset as the starting point
    //指定结束偏移量作为起点
    testBadOptions("startingOffsets" -> "latest")("starting offset can't be latest " +
      "for batch queries on Kafka")

    // Now do it with an explicit json start offset indicating latest
    //现在用明确的json起始偏移量来表示最新的
    val startPartitionOffsets = Map( new TopicPartition("t", 0) -> -1L)
    val startingOffsets = JsonUtils.partitionOffsets(startPartitionOffsets)
    testBadOptions("subscribe" -> "t", "startingOffsets" -> startingOffsets)(
      "startingOffsets for t-0 can't be latest for batch queries on Kafka")


    // Make sure we catch ending offsets that indicate earliest
    //确保我们抓住表示最早的结束偏移量
    testBadOptions("endingOffsets" -> "earliest")("ending offset can't be earliest " +
      "for batch queries on Kafka")

    // Make sure we catch ending offsets that indicating earliest
    //确保我们抓住表明最早的结束偏移量
    val endPartitionOffsets = Map(new TopicPartition("t", 0) -> -2L)
    val endingOffsets = JsonUtils.partitionOffsets(endPartitionOffsets)
    testBadOptions("subscribe" -> "t", "endingOffsets" -> endingOffsets)(
      "ending offset for t-0 can't be earliest for batch queries on Kafka")

    // No strategy specified
    //没有指定策略
    testBadOptions()("options must be specified", "subscribe", "subscribePattern")

    // Multiple strategies specified
    //指定多个策略
    testBadOptions("subscribe" -> "t", "subscribePattern" -> "t.*")(
      "only one", "options can be specified")

    testBadOptions("subscribe" -> "t", "assign" -> """{"a":[0]}""")(
      "only one", "options can be specified")

    testBadOptions("assign" -> "")("no topicpartitions to assign")
    testBadOptions("subscribe" -> "")("no topics to subscribe")
    testBadOptions("subscribePattern" -> "")("pattern to subscribe is empty")
  }
}
