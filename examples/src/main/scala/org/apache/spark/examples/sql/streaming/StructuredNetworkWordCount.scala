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
package org.apache.spark.examples.sql.streaming

import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network.
 *
 * Usage: StructuredNetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.StructuredNetworkWordCount
 *    localhost 9999`
 */
object StructuredNetworkWordCount {
  def main(args: Array[String]) {
    println("====")
    if (args.length < 2) {
      System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    //// 创建表示从连接到 localhost:9999 的输入行 stream 的 DataFrame
    // Create DataFrame representing the stream of input lines from connection to host:port
  val lines = spark.readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load()
  // 将 lines 切分为 words
  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))
  // 生成正在运行的 word count
  // Generate running word count
  val wordCounts = words.groupBy("value").count()
  // 开始运行将 running counts 打印到控制台的查询
  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    //Complete Mode（完全模式）- 输出最新的完整的结果表数据
    //Append Mode（附加模式） - 只输出结果表中本批次新增的数据,其实也就是本批次中的数据；
    //Update Mode（更新模式） -只输出结果表中被本批次修改的数据
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()
}
}
// scalastyle:on println
