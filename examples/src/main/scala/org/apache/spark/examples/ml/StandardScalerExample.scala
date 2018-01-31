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
import org.apache.spark.ml.feature.StandardScaler
// $example off$

object StandardScalerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {
    // $example on$
    val dataFrame = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
    //StandardScaler处理的对象是每一列，也就是每一维特征，将特征标准化为单位标准差或是0均值，或是0均值单位标准差
    //withStd默认为真。将数据标准化到单位标准差。
    //withMean: 默认为假。是否变换为0均值。
    //StandardScaler需要fit数据，获取每一维的均值和标准差，来缩放每一维特征。
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)//数据标准化到单位标准差
      .setWithMean(false) //变换均值

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)

    scaledData.show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
