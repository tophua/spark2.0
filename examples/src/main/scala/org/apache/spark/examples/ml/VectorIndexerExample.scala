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
import org.apache.spark.ml.feature.VectorIndexer
// $example off$

object VectorIndexerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    // $example on$
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    /**
      * 提高决策树或随机森林等ML方法的分类效果
      * VectorIndexer是对数据集特征向量中的类别（离散值）特征(index categorical features categorical features)进行编号
      * 它能够自动判断那些特征是离散值型的特征,并对他们进行编号,具体做法是通过设置一个maxCategories,
      * 特征向量中某一个特征不重复取值个数小于maxCategories,则被重新编号为0～K(K<=maxCategories-1),
      * 某一个特征不重复取值个数大于maxCategories,则该特征视为连续值,不会重新编号(不会发生任何改变)
      * +-------------------------+-------------------------+
        |features                 |indexedFeatures          |
        +-------------------------+-------------------------+
      * |(3,[0,1,2],[2.0,5.0,7.0])|(3,[0,1,2],[2.0,1.0,1.0])|
      * +-------------------------+-------------------------+
      * 结果分析：特征向量包含3个特征,即特征0,特征1,特征2,如Row=1,对应的特征分别是2.0,5.0,7.0.被转换为2.0,1.0,1.0。
      * 发现只有特征1,特征2被转换了,特征0没有被转换,这是因为特征0有6中取值(2，3，4，5，8，9),多于前面的设置setMaxCategories(5)
      * 因此被视为连续值了，不会被转换。
      *  特征1中，（4，5，6，7，9）-->(0,1,2,3,4,5)
      *  特征2中,  (2,7,9)-->(0,1,2)
      * 输出DataFrame格式说明（Row=1）：
      *  3个特征 特征0，1，2      转换前的值
      *  |(3,    [0,1,2],       [2.0,5.0,7.0])
      *  3个特征 特征1，1，2       转换后的值
      *  |(3,    [0,1,2],      [2.0,1.0,1.0])|
      */
    val indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)

    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Chose ${categoricalFeatures.size} categorical features: " +
      categoricalFeatures.mkString(", "))

    // Create new column "indexed" with categorical values transformed to indices
    val indexedData = indexerModel.transform(data)
    indexedData.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
