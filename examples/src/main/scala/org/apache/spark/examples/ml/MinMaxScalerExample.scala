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
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
// $example off$

object MinMaxScalerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    // $example on$
    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")
   // MinMaxScaler作用同样是每一列,即每一维特征,将每一维特征线性地映射到指定的区间,通常是[0, 1]
   // 它也有两个参数可以设置：
   // - min: 默认为0,指定区间的下限
   // - max: 默认为1,指定区间的上限
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // Compute summary statistics and generate MinMaxScalerModel
    val scalerModel = scaler.fit(dataFrame)

    // rescale each feature to range [min, max].
    val scaledData = scalerModel.transform(dataFrame)

  /*  +---+--------------+--------------+
    | id|      features|scaledFeatures|
    +---+--------------+--------------+
    |  0|[1.0,0.1,-1.0]| [0.0,0.0,0.0]|
    |  1| [2.0,1.1,1.0]| [0.5,0.1,0.5]|
    |  2|[3.0,10.1,3.0]| [1.0,1.0,1.0]|
    +---+--------------+--------------+*/
    scaledData.show()
    //Features scaled to range: [0.0, 1.0]
    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
      /**
        +--------------+--------------+
        |      features|scaledFeatures|
        +--------------+--------------+
        |[1.0,0.1,-1.0]| [0.0,0.0,0.0]|
        | [2.0,1.1,1.0]| [0.5,0.1,0.5]|
        |[3.0,10.1,3.0]| [1.0,1.0,1.0]|
        +--------------+--------------+
      **/
    scaledData.select("features", "scaledFeatures").show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
