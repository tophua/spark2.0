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

import java.io.{File, IOException}
import java.lang.{Integer => JInt}
import java.net.InetSocketAddress
import java.util.{Map => JMap, Properties}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.Random

import kafka.admin.AdminUtils
import kafka.api.Request
import kafka.common.TopicAndPartition
import kafka.server.{KafkaConfig, KafkaServer, OffsetCheckpoint}
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * This is a helper class for Kafka test suites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
  * 这是一个Kafka测试套件的辅助类,这具有设置和拆除当地Kafka服务器的功能,并使用Kafka生产者推送数据
 *
 * The reason to put Kafka test utility class in src is to test Python related Kafka APIs.
  * 在src中放入Kafka测试工具类的原因是测试Python相关的Kafka API
 */
class KafkaTestUtils(withBrokerProps: Map[String, Object] = Map.empty) extends Logging {

  // Zookeeper related configurations
  //Zookeeper相关的配置
  private val zkHost = "localhost"
  private var zkPort: Int = 0
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 6000

  private var zookeeper: EmbeddedZookeeper = _

  private var zkUtils: ZkUtils = _

  // Kafka broker related configurations
  // Kafka broker相关配置
  private val brokerHost = "localhost"
  private var brokerPort = 0
  private var brokerConf: KafkaConfig = _

  // Kafka broker server
  private var server: KafkaServer = _

  // Kafka producer
  //Kafka 生产者
  private var producer: Producer[String, String] = _

  // Flag to test whether the system is correctly started
  //标记以测试系统是否正确启动
  private var zkReady = false
  private var brokerReady = false

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$brokerHost:$brokerPort"
  }

  def zookeeperClient: ZkUtils = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper client")
    Option(zkUtils).getOrElse(
      throw new IllegalStateException("Zookeeper client is not yet initialized"))
  }

  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  //设置Embedded Zookeeper服务器并获取适当的Zookeeper端口
  private def setupEmbeddedZookeeper(): Unit = {
    // Zookeeper server startup
    //Zookeeper服务器启动
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    zkUtils = ZkUtils(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout, false)
    zkReady = true
  }

  // Set up the Embedded Kafka server
  //设置嵌入式Kafka服务器
  private def setupEmbeddedKafkaServer(): Unit = {
    assert(zkReady, "Zookeeper should be set up beforehand")

    // Kafka broker startup
    //Kafka broker启动
    Utils.startServiceOnPort(brokerPort, port => {
      brokerPort = port
      brokerConf = new KafkaConfig(brokerConfiguration, doLog = false)
      server = new KafkaServer(brokerConf)
      server.startup()
      brokerPort = server.boundPort()
      (server, brokerPort)
    }, new SparkConf(), "KafkaBroker")

    brokerReady = true
  }

  /** setup the whole embedded servers, including Zookeeper and Kafka brokers
    * 设置整个嵌入式服务器,包括Zookeeper和Kafka经纪人 */
  def setup(): Unit = {
    setupEmbeddedZookeeper()
    setupEmbeddedKafkaServer()
  }

  /** Teardown the whole servers, including Kafka broker and Zookeeper
    * 拆除整个服务器,包括Kafka broker和Zookeeper */
  def teardown(): Unit = {
    brokerReady = false
    zkReady = false

    if (producer != null) {
      producer.close()
      producer = null
    }

    if (server != null) {
      server.shutdown()
      server.awaitShutdown()
      server = null
    }

    // On Windows, `logDirs` is left open even after Kafka server above is completely shut down
    // in some cases. It leads to test failures on Windows if the directory deletion failure
    // throws an exception.
    //在Windows上,即使在上面的Kafka服务器完全关闭之后,logDirs仍然是打开的。
    //如果目录删除失败引发异常,则会导致Windows上的测试失败。
    brokerConf.logDirs.foreach { f =>
      try {
        Utils.deleteRecursively(new File(f))
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
    }

    if (zkUtils != null) {
      zkUtils.close()
      zkUtils = null
    }

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster
    * 创建一个Kafka主题,并等到它传播到整个集群 */
  def createTopic(topic: String, partitions: Int, overwrite: Boolean = false): Unit = {
    var created = false
    while (!created) {
      try {
        AdminUtils.createTopic(zkUtils, topic, partitions, 1)
        created = true
      } catch {
        case e: kafka.common.TopicExistsException if overwrite => deleteTopic(topic)
      }
    }
    // wait until metadata is propagated
    //等到元数据被传播
    (0 until partitions).foreach { p =>
      waitUntilMetadataIsPropagated(topic, p)
    }
  }

  def getAllTopicsAndPartitionSize(): Seq[(String, Int)] = {
    zkUtils.getPartitionsForTopics(zkUtils.getAllTopics()).mapValues(_.size).toSeq
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster
    * 创建一个Kafka主题，并等到它传播到整个集群*/
  def createTopic(topic: String): Unit = {
    createTopic(topic, 1)
  }

  /** Delete a Kafka topic and wait until it is propagated to the whole cluster
    * 删除一个Kafka主题，并等待它传播到整个集群*/
  def deleteTopic(topic: String): Unit = {
    val partitions = zkUtils.getPartitionsForTopics(Seq(topic))(topic).size
    AdminUtils.deleteTopic(zkUtils, topic)
    verifyTopicDeletionWithRetries(zkUtils, topic, partitions, List(this.server))
  }

  /** Add new paritions to a Kafka topic
    * 将新分区添加到Kafka主题*/
  def addPartitions(topic: String, partitions: Int): Unit = {
    AdminUtils.addPartitions(zkUtils, topic, partitions)
    // wait until metadata is propagated
    (0 until partitions).foreach { p =>
      waitUntilMetadataIsPropagated(topic, p)
    }
  }

  /** Java-friendly function for sending messages to the Kafka broker
    * 用于将消息发送给Kafka代理的Java友好功能*/
  def sendMessages(topic: String, messageToFreq: JMap[String, JInt]): Unit = {
    sendMessages(topic, Map(messageToFreq.asScala.mapValues(_.intValue()).toSeq: _*))
  }

  /** Send the messages to the Kafka broker
    * 发送消息给卡Kafka broker*/
  def sendMessages(topic: String, messageToFreq: Map[String, Int]): Unit = {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }

  /** Send the array of messages to the Kafka broker
    * 将消息数组发送给Kafka代理*/
  def sendMessages(topic: String, messages: Array[String]): Seq[(String, RecordMetadata)] = {
    sendMessages(topic, messages, None)
  }

  /** Send the array of messages to the Kafka broker using specified partition
    * 使用指定的分区将消息数组发送到Kafka代理 */
  def sendMessages(
      topic: String,
      messages: Array[String],
      partition: Option[Int]): Seq[(String, RecordMetadata)] = {
    producer = new KafkaProducer[String, String](producerConfiguration)
    val offsets = try {
      messages.map { m =>
        val record = partition match {
          case Some(p) => new ProducerRecord[String, String](topic, p, null, m)
          case None => new ProducerRecord[String, String](topic, m)
        }
        val metadata =
          producer.send(record).get(10, TimeUnit.SECONDS)
          logInfo(s"\tSent $m to partition ${metadata.partition}, offset ${metadata.offset}")
        (m, metadata)
      }
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
    offsets
  }

  def cleanupLogs(): Unit = {
    server.logManager.cleanupLogs()
  }

  def getEarliestOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    val kc = new KafkaConsumer[String, String](consumerConfiguration)
    logInfo("Created consumer to get earliest offsets")
    kc.subscribe(topics.asJavaCollection)
    kc.poll(0)
    val partitions = kc.assignment()
    kc.pause(partitions)
    kc.seekToBeginning(partitions)
    val offsets = partitions.asScala.map(p => p -> kc.position(p)).toMap
    kc.close()
    logInfo("Closed consumer to get earliest offsets")
    offsets
  }

  def getLatestOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    val kc = new KafkaConsumer[String, String](consumerConfiguration)
    logInfo("Created consumer to get latest offsets")
    kc.subscribe(topics.asJavaCollection)
    kc.poll(0)
    val partitions = kc.assignment()
    kc.pause(partitions)
    kc.seekToEnd(partitions)
    val offsets = partitions.asScala.map(p => p -> kc.position(p)).toMap
    kc.close()
    logInfo("Closed consumer to get latest offsets")
    offsets
  }

  protected def brokerConfiguration: Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("advertised.host.name", "localhost")
    props.put("port", brokerPort.toString)
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props.put("delete.topic.enable", "true")
    props.put("offsets.topic.num.partitions", "1")
    props.putAll(withBrokerProps.asJava)
    props
  }

  private def producerConfiguration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddress)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[StringSerializer].getName)
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all")
    props
  }

  private def consumerConfiguration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddress)
    props.put("group.id", "group-KafkaTestUtils-" + Random.nextInt)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("enable.auto.commit", "false")
    props
  }

  /** Verify topic is deleted in all places, e.g, brokers, zookeeper.
    * 验证主题在所有地方被删除，例如brokers，zookeeper。 */
  private def verifyTopicDeletion(
      topic: String,
      numPartitions: Int,
      servers: Seq[KafkaServer]): Unit = {
    val topicAndPartitions = (0 until numPartitions).map(TopicAndPartition(topic, _))

    import ZkUtils._
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    //等到删除话题的管理路径被删除，信令完成话题删除
    assert(
      !zkUtils.pathExists(getDeleteTopicPath(topic)),
      s"${getDeleteTopicPath(topic)} still exists")
    assert(!zkUtils.pathExists(getTopicPath(topic)), s"${getTopicPath(topic)} still exists")
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    //确保主题分区已从所有代理的副本管理器中删除
    assert(servers.forall(server => topicAndPartitions.forall(tp =>
      server.replicaManager.getPartition(tp.topic, tp.partition) == None)),
      s"topic $topic still exists in the replica manager")
    // ensure that logs from all replicas are deleted if delete topic is marked successful
    //确保删除主题标记为成功时，删除所有副本中的日志
    assert(servers.forall(server => topicAndPartitions.forall(tp =>
      server.getLogManager().getLog(tp).isEmpty)),
      s"topic $topic still exists in log mananger")
    // ensure that topic is removed from all cleaner offsets
    //确保从所有清除偏移中删除主题
    assert(servers.forall(server => topicAndPartitions.forall { tp =>
      val checkpoints = server.getLogManager().logDirs.map { logDir =>
        new OffsetCheckpoint(new File(logDir, "cleaner-offset-checkpoint")).read()
      }
      checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.contains(tp))
    }), s"checkpoint for topic $topic still exists")
    // ensure the topic is gone
    //确保话题消失
    assert(
      !zkUtils.getAllTopics().contains(topic),
      s"topic $topic still exists on zookeeper")
  }

  /** Verify topic is deleted. Retry to delete the topic if not.
    * 确认主题已删除。 如果不是，请重试删除主题*/
  private def verifyTopicDeletionWithRetries(
      zkUtils: ZkUtils,
      topic: String,
      numPartitions: Int,
      servers: Seq[KafkaServer]) {
    eventually(timeout(60.seconds), interval(200.millis)) {
      try {
        verifyTopicDeletion(topic, numPartitions, servers)
      } catch {
        case e: Throwable =>
          // As pushing messages into Kafka updates Zookeeper asynchronously, there is a small
          // chance that a topic will be recreated after deletion due to the asynchronous update.
          // Hence, delete the topic and retry.
          AdminUtils.deleteTopic(zkUtils, topic)
          throw e
      }
    }
  }
  //等到元数据被传播
  private def waitUntilMetadataIsPropagated(topic: String, partition: Int): Unit = {
    def isPropagated = server.apis.metadataCache.getPartitionInfo(topic, partition) match {
      case Some(partitionState) =>
        val leaderAndInSyncReplicas = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr

        zkUtils.getLeaderForPartition(topic, partition).isDefined &&
          Request.isValidBrokerId(leaderAndInSyncReplicas.leader) &&
          leaderAndInSyncReplicas.isr.nonEmpty

      case _ =>
        false
    }
    eventually(timeout(60.seconds)) {
      assert(isPropagated, s"Partition [$topic, $partition] metadata not propagated after timeout")
    }
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      // The directories are not closed even if the ZooKeeper server is shut down.
      // Please see ZOOKEEPER-1844, which is fixed in 3.4.6+. It leads to test failures
      // on Windows if the directory deletion failure throws an exception.
      try {
        Utils.deleteRecursively(snapshotDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
      try {
        Utils.deleteRecursively(logDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
    }
  }
}

