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

package org.apache.spark.ml.feature

import scala.beans.BeanInfo

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

@BeanInfo
case class NGramTestData(inputTokens: Array[String], wantedNGrams: Array[String])

/**
  * n-gram代表由n个字组成的句子。利用上下文中相邻词间的搭配信息，在需要把连续无空格的拼音、笔划，或代表字母或笔划的数字，转换成汉字串(即句子)时，
  * 可以计算出具有最大概率的句子，从而实现到汉字的自动转换，无需用户手动选择，避开了许多汉字对应一个相同的拼音(或笔划串，或数字串)的重码问题。
  * 该模型基于这样一种假设，第N个词的出现只与前面N-1个词相关，而与其它任何词都不相关，整句的概率就是各个词出现概率的乘积。
  */
class NGramSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import org.apache.spark.ml.feature.NGramSuite._
  import testImplicits._
  //默认行为产生两个特征
  test("default behavior yields bigram features") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
    val dataset = Seq(NGramTestData(
      Array("Test", "for", "ngram", "."),
      Array("Test for", "for ngram", "ngram .")
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("NGramLength=4 yields length 4 n-grams") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e"),
      Array("a b c d", "b c d e")
    )).toDF()
    testNGram(nGram, dataset)
  }
  //空输入产生空输出
  test("empty input yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = Seq(NGramTestData(Array(), Array())).toDF()
    testNGram(nGram, dataset)
  }

  test("input array < n yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(6)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e"),
      Array()
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("read/write") {
    val t = new NGram()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setN(3)
    testDefaultReadWrite(t)
  }
}

object NGramSuite extends SparkFunSuite {

  def testNGram(t: NGram, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("nGrams", "wantedNGrams")
      .collect()
      .foreach { case Row(actualNGrams, wantedNGrams) =>
        assert(actualNGrams === wantedNGrams)
      }
  }
}
