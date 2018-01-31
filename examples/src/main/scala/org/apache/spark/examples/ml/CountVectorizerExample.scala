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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
// $example off$

object CountVectorizerExample extends SparkCommant{
  def main(args: Array[String]) {

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    /**
      * CountVectorizer旨在通过计数来将一个文档转换为向量,
      * Countvectorizer作为Estimator提取词汇进行训练,并生成一个CountVectorizerModel用于存储相应的词汇向量空间。
      * 该模型产生文档关于词语的稀疏表示,其表示可以传递给其他算法
      * CountVectorizer将根据语料库中的词频排序从高到低进行选择,词汇表的最大含量由vocabsize超参数来指定,超参数minDF,
      * 则指定词汇表中的词语至少要在多少个不同文档中出现
      */
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features")

    /**
      +---+---------------+-------------------------+
      |id |words          |features                 |
      +---+---------------+-------------------------+
      |0  |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
      |1  |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
      +---+---------------+-------------------------+
      */
    cvModel.transform(df).show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println


