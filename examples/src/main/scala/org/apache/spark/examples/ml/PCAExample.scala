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
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
// $example off$

object PCAExample extends SparkCommant{
  def main(args: Array[String]): Unit = {


    // $example on$
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //PCA可以将特征向量投影到低维空间,实现对特征向量的降维。
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)
/*    +---------------------+-----------------------------------------------------------+
    |features             |pcaFeatures                                                |
    +---------------------+-----------------------------------------------------------+
    |(5,[1,3],[1.0,7.0])  |[1.6485728230883807,-4.013282700516296,-5.524543751369388] |
    |[2.0,0.0,3.0,4.0,5.0]|[-4.645104331781534,-1.1167972663619026,-5.524543751369387]|
    |[4.0,0.0,0.0,6.0,7.0]|[-6.428880535676489,-5.337951427775355,-5.524543751369389] |
    +---------------------+-----------------------------------------------------------+*/
    pca.transform(df).show(false)
    val result = pca.transform(df).select("pcaFeatures")
    //result.show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
