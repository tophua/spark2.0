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

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network over a
 * sliding window of configurable duration. Each line from the network is tagged
 * with a timestamp that is used to determine the windows into which it falls.
  *
  * 以UTF8编码的字符计数'\ n'分隔的文本,通过可配置持续时间的滑动窗口从网络接收,
  * 来自网络的每一行都标有一个时间戳,用于确定它所属的窗口。
 *
 * Usage: StructuredNetworkWordCountWindowed <hostname> <port> <window duration>
 *   [<slide duration>]
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data. 描述结构化流传输将连接到的接收数据的TCP服务器
 * <window duration> gives the size of window, specified as integer number of seconds
  *                  给出窗口的大小,指定为整数秒
 * <slide duration> gives the amount of time successive windows are offset from one another,
 * given in the same units as above.
  * 给出连续窗口相互偏移的时间量，以与上述相同的单位给出。<slide duration> should be less than or equal to
 * <window duration>. If the two are equal, successive windows have no overlap. If
 * <slide duration> is not provided, it defaults to <window duration>.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example sql.streaming.StructuredNetworkWordCountWindowed
 *    localhost 9999 <window duration in seconds> [<slide duration in seconds>]`
 *
 * One recommended <window duration>, <slide duration> pair is 10, 5
 */
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
        " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    //创建DataFrame,表示从连接到主机：端口的输入行的流
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    //将这些行拆分成单词,保留时间戳
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    //通过窗口和单词分组数据,并计算每个组的计数
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    //开始运行查询,打印窗口的字数到控制台
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
