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
package org.apache.spark.examples

import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession

/**
 * Simple test for reading and writing to a distributed
 * file system.  This example does the following:
 * 简单测试的分布式文件系统读写,这个例子做以下
 *   1. Reads local file 读取本地文件
 *   2. Computes word count on local file 计算本地文件上的字计数
 *   3. Writes local file to a DFS 写本地文件到DFS
 *   4. Reads the file back from the DFS 将文件从DFS读回来
 *   5. Computes word count on the file using Spark 用Spark计算文件上的字计数
 *   6. Compares the word count results 比较字计数结果
 */
object DFSReadWriteTest {

  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""

  private val NPARAMS = 2
  /**
    * 本地文件读取
    */
  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }
  /**
    * 打印使用
    */
  private def printUsage(): Unit = {
    val usage: String = "DFS Read-Write Test\n" +
    "\n" +
    "Usage: localFile dfsDir\n" +
    "\n" +
    "localFile - (string) local file to use in test\n" +
    "dfsDir - (string) DFS directory for read/write tests\n"

    println(usage)
  }
  /**
    * 解析参数判断是否文件
    */
  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {//如果参数不相于2则退出
      printUsage()
      System.exit(1)
    }

    var i = 0
    //取出第一个参数文件
    localFilePath = new File(args(i))
    //如果文件不存在,则给定的路径文件不存在
    if (!localFilePath.exists) {
      System.err.println("Given path (" + args(i) + ") does not exist.\n")
      printUsage()
      System.exit(1)
    }
    //如果文件不是文件,则给定的路径不是文件
    if (!localFilePath.isFile) {
      System.err.println("Given path (" + args(i) + ") is not a file.\n")
      printUsage()
      System.exit(1)
    }

    i += 1
    //存放hdfs文件位置的目录
    dfsDirPath = args(i)
  }
  /**
    * 运行本地单词计数
    */
  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" ")) //以空格分隔
      .flatMap(_.split("\t"))//水平制表(HT)(跳到下一个TAB位置)
      .filter(_.nonEmpty)//过滤掉长度大小于0
      .groupBy(w => w)//分组
      .mapValues(_.size)//大小
      .values //转换值
      .sum //求和
  }
  //第一参数读取文件,第二参数是存文件目录
  def main(args: Array[String]): Unit = {
    parseArgs(args)

    println("Performing local word count")
    //执行本地字计数
    val fileContents = readFile(localFilePath.toString())
    //读取文件
    val localWordCount = runLocalWordCount(fileContents)

    val HADOOP_USER = "hdfs"
    // 设置访问spark使用的用户名
    System.setProperty("user.name", HADOOP_USER);
    // 设置访问hadoop使用的用户名
    System.setProperty("HADOOP_USER_NAME", HADOOP_USER);
    //创建Spark配置文件

    println("Creating SparkSession")
    val spark = SparkSession
      .builder
      .appName("DFS Read Write Test")
      .getOrCreate()

    println("Writing local file to DFS")
    //写本地文件到DFS
    val dfsFilename = dfsDirPath + "/dfs_read_write_test" //存放读取文件的位置
    val fileRDD = spark.sparkContext.parallelize(fileContents)
    //保存文件
    fileRDD.saveAsTextFile(dfsFilename)

    println("Reading file from DFS and running Word Count")
    //从DFS阅读文件和运行字数
    val readFileRDD = spark.sparkContext.textFile(dfsFilename)

    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum

    spark.stop()
    //如果本地单词统计数和dfs单词统计数相同,则表示数据读取成功
    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) agree.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) disagree.")
    }

  }
}
// scalastyle:on println
