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
import org.apache.spark.ml.regression.LinearRegression
// $example off$
import org.apache.spark.sql.SparkSession
/**
  * 线性回归网络神经例子
  */
object LinearRegressionWithElasticNetExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithElasticNetExample")
      .getOrCreate()

    // $example on$
    // Load training data
    val training = spark.read.format("libsvm")
      .load("data/mllib/sample_linear_regression_data.txt")

    val lr = new LinearRegression()
      .setMaxIter(10)//设置最大迭代次数
      .setRegParam(0.3)//设置正则化参数
      .setElasticNetParam(0.8)//ElasticNetParam=0.0为L2正则化 1.0为L1正则化

    // Fit the model
    //fit()方法将DataFrame转化为一个Transformer的算法
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    //打印线性回归的系数和截距
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    //总结训练集上的模型并打印出一些指标
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    //rmse均方根误差说明样本的离散程度
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //R2平方系统也称判定系数,用来评估模型拟合数据的好坏
    println(s"r2: ${trainingSummary.r2}")
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
