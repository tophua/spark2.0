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
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
// $example off$

object PolynomialExpansionExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    // $example on$
    /**
      * 下面的示例会介绍如何将你的特征集拓展到3维多项式空间
      */
    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //多项式扩展(Polynomial expansion)是将n维的原始特征组合扩展到多项式空间的过程
    //在输入数据中增加非线性特征可以有效的提高模型的复杂度,简单且常用的方法就是使用多项式特征(polynomial features),可以得到特征的高阶交叉项：
    val polyExpansion = new PolynomialExpansion()
      .setInputCol("features")//输入列
      .setOutputCol("polyFeatures")//输出列
      .setDegree(3)

    val polyDF = polyExpansion.transform(df)

    /**
      +----------+------------------------------------------+
      |features  |polyFeatures                              |
      +----------+------------------------------------------+
      |[2.0,1.0] |[2.0,4.0,8.0,1.0,2.0,4.0,1.0,2.0,1.0]     |
      |[0.0,0.0] |[0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]     |
      |[3.0,-1.0]|[3.0,9.0,27.0,-1.0,-3.0,-9.0,1.0,3.0,-1.0]|
      +----------+------------------------------------------+
      */
    polyDF.show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
