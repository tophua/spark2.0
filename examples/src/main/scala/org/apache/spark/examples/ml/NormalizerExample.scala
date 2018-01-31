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
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
// $example off$
import org.apache.spark.sql.SparkSession

object NormalizerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("NormalizerExample")
      .getOrCreate()

    // $example on$
    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    // Normalize each Vector using $L^1$ norm.
    //特征向量正则化
    //Normalizer是一个转换器,它可以将一组特征向量（通过计算p-范数）规范化。
    // 参数为p（默认值：2）来指定规范化中使用的p-norm。规范化操作可以使输入数据标准化
    //

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(dataFrame)
    println("Normalized using L^1 norm")

    /**
        +---+--------------+------------------+
        | id|      features|      normFeatures|
        +---+--------------+------------------+
        |  0|[1.0,0.5,-1.0]|    [0.4,0.2,-0.4]|
        |  1| [2.0,1.0,1.0]|   [0.5,0.25,0.25]|
        |  2|[4.0,10.0,2.0]|[0.25,0.625,0.125]|
        +---+--------------+------------------+
      */
    l1NormData.show()

    // Normalize each Vector using $L^\infty$ norm.
    val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
    println("Normalized using L^inf norm")

    /**
      +---+--------------+--------------+
      | id|      features|  normFeatures|
      +---+--------------+--------------+
      |  0|[1.0,0.5,-1.0]|[1.0,0.5,-1.0]|
      |  1| [2.0,1.0,1.0]| [1.0,0.5,0.5]|
      |  2|[4.0,10.0,2.0]| [0.4,1.0,0.2]|
      +---+--------------+--------------+
      */
    lInfNormData.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
