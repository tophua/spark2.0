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
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
// $example off$
/**
  * ElementwiseProduct点乘,就是说每个矩阵元素对应相乘
  */
object ElementwiseProductExample extends SparkCommant{
  def main(args: Array[String]): Unit = {


    // $example on$
    // Create some vector data; also works for sparse vectors
    //创建一些向量数据,也适用于稀疏向量
    val dataFrame = spark.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    /**
      * ElementwiseProduct对输入向量的每个元素乘以一个权重向量的每个元素,对输入向量每个元素逐个进行放缩
      */
    //ElementwiseProduct 点乘,就是说每个矩阵元素对应相乘
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // Batch transform the vectors to create new column:
    /**
      +---+-------------+-----------------+
      | id|       vector|transformedVector|
      +---+-------------+-----------------+
      |  a|[1.0,2.0,3.0]|    [0.0,2.0,6.0]|
      |  b|[4.0,5.0,6.0]|   [0.0,5.0,12.0]|
      +---+-------------+-----------------+
      **/
    transformer.transform(dataFrame).show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
