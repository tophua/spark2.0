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
package org.apache.spark.examples.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * Custom Receiver that receives data over a socket. Received bytes are interpreted as
 * text and \n delimited lines are considered as records. They are then counted and printed.
 * 在套接字上接收数据的自定义接收器,接收字节被解释为文本和\ n分隔线被认为是记录,然后,他们计算和打印
 * To run this on your local machine, you need to first run a Netcat server
 *  在你的本地机器上运行这个,你需要先运行一个netcat服务器
  *  `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.CustomReceiver localhost 9999`
 */
object CustomReceiver {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: CustomReceiver <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    //创建上下文以1秒为批大小
    val sparkConf = new SparkConf().setAppName("CustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create an input stream with the custom receiver on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    //在目标IP上创建一个具有自定义接收器的输入流：端口,并在“n”分隔的文本的输入流中计数单词
    //自定义接收器,receiverStream创建PluggableInputDStream
    val lines = ssc.receiverStream(new CustomReceiver(args(0), args(1).toInt))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

//自定义接收数据
class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    //多线程接收
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
    //没有什么可做的线程调用receive()
    // is designed to stop by itself isStopped() returns false
    //是为了阻止自己isstopped()返回false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  //创建一个套接字连接并接收数据,直到停止接收为止
  private def receive() {
   var socket: Socket = null
   var userInput: String = null
   try {
     logInfo("Connecting to " + host + ":" + port)
     socket = new Socket(host, port)
     logInfo("Connected to " + host + ":" + port)
     val reader = new BufferedReader(
       new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
     userInput = reader.readLine()
     while(!isStopped && userInput != null) {
       store(userInput)//存储数据
       userInput = reader.readLine() //读取数据
     }
     reader.close()//关闭流
     socket.close()//关闭连接
     logInfo("Stopped receiving")
     restart("Trying to connect again")
   } catch {
     case e: java.net.ConnectException =>
       restart("Error connecting to " + host + ":" + port, e)
     case t: Throwable =>
       restart("Error receiving data", t)
   }
  }
}
// scalastyle:on println
