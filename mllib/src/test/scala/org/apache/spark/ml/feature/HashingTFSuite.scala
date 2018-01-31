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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{HashingTF => MLlibHashingTF}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils
//spark 的词频计算是用特征哈希（HashingTF）来计算的。特征哈希是一种处理高维数据的技术,
/**
  * TF-IDF算法从文本分词中创建特征向量
  * HashTF从一个文档中计算出给定大小的词频向量。为了将词和向量顺序对应起来,所以使用了哈希。
  * HashingTF使用每个单词对所需向量的长度S取模得出的哈希值,把所有单词映射到一个0到S-1之间的数字上。
  * 由此可以保证生成一个S维的向量,随后当构建好词频向量后,使用IDF来计算逆文档频率,然后将它们与词频相乘计算TF-IDF
  */
class HashingTFSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._
  import HashingTFSuite.murmur3FeatureIdx

  test("params") {
    ParamsSuite.checkParams(new HashingTF)
  }

  test("hashingTF") {
    val df = Seq((0, "a a b b c d".split(" ").toSeq)).toDF("id", "words")
    val n = 100
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(n)
    val output = hashingTF.transform(df)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    //属性分组
    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    require(attrGroup.numAttributes === Some(n))
    val features = output.select("features").first().getAs[Vector](0)
    // Assume perfect hash on "a", "b", "c", and "d".
    def idx: Any => Int = murmur3FeatureIdx(n)
    val expected = Vectors.sparse(n,
      Seq((idx("a"), 2.0), (idx("b"), 2.0), (idx("c"), 1.0), (idx("d"), 1.0)))
    assert(features ~== expected absTol 1e-14)
  }
  //应用二项式频率
  test("applying binary term freqs") {
    val df = Seq((0, "a a b c c c".split(" ").toSeq)).toDF("id", "words")
    val n = 100
    val hashingTF = new HashingTF()
        .setInputCol("words")
        .setOutputCol("features")
        .setNumFeatures(n)
        .setBinary(true)
    val output = hashingTF.transform(df)
    val features = output.select("features").first().getAs[Vector](0)
    //假设输入特征是完美的哈希
    def idx: Any => Int = murmur3FeatureIdx(n)  // Assume perfect hash on input features
    val expected = Vectors.sparse(n,
      Seq((idx("a"), 1.0), (idx("b"), 1.0), (idx("c"), 1.0)))
    assert(features ~== expected absTol 1e-14)
  }

  test("read/write") {
    val t = new HashingTF()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setNumFeatures(10)
    testDefaultReadWrite(t)
  }

}

object HashingTFSuite {

  private[feature] def murmur3FeatureIdx(numFeatures: Int)(term: Any): Int = {
    Utils.nonNegativeMod(MLlibHashingTF.murmur3Hash(term), numFeatures)
  }

}
