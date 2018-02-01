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

package org.apache.spark.examples.ml

// scalastyle:off println

// $example on$
import org.apache.spark.ml.clustering.BisectingKMeans
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating bisecting k-means clustering.
  * 证明二分k均值算法聚类的一个例子
  * 二分k均值算法,它是k-means聚类算法的一个变体,主要是为了改进k-means算法随机选择初始质心的随机性造成聚类结果不确定性的问题,
  * 而Bisecting k-means算法受随机选择初始质心的影响比较小
 * Run with
 * {{{
 * bin/run-example ml.BisectingKMeansExample
 * }}}
 */
object BisectingKMeansExample {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    val spark = SparkSession
      .builder
      .appName("BisectingKMeansExample")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    // Trains a bisecting k-means model.
    //训练一个平分的k-means模型
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val model = bkm.fit(dataset)

    // Evaluate clustering.
    //评估聚类
    val cost = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    // Shows the result.
    //显示结果
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println

