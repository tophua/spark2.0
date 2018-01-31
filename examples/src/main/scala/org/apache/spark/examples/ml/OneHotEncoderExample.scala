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
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
// $example off$

object OneHotEncoderExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)
    /**
      * 离散<->连续特征或Label相互转换
      * 独热编码将类别特征(离散的,已经转换为数字编号形式),映射成独热编码。
      * 这样在诸如Logistic回归这样需要连续数值值作为特征输入的分类器中也可以
      * 独热编码即 One-Hot 编码,又称一位有效编码
      * 例如： 自然状态码为：000,001,010,011,100,101
      *  独热编码为：000001,000010,000100,001000,010000,100000
      *  可以这样理解，对于每一个特征，如果它有m个可能值，那么经过独 热编码
      *  后，就变成了m个二元特征。并且，这些特征互斥，每次只有 一个激活。因
      *  此，数据会变成稀疏的。
      *  这样做的好处主要有：
      *  解决了分类器不好处理属性数据的问题
      *  在一定程度上也起到了扩充特征的作用
      */
    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)

    /**
      +---+--------+-------------+-------------+
      | id|category|categoryIndex|  categoryVec|
      +---+--------+-------------+-------------+
      |  0|       a|          0.0|(2,[0],[1.0])|
      |  1|       b|          2.0|    (2,[],[])|
      |  2|       c|          1.0|(2,[1],[1.0])|
      |  3|       a|          0.0|(2,[0],[1.0])|
      |  4|       a|          0.0|(2,[0],[1.0])|
      |  5|       c|          1.0|(2,[1],[1.0])|
      +---+--------+-------------+-------------+
      */
    encoded.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
