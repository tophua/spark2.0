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
import org.apache.spark.ml.regression.GeneralizedLinearRegression
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating generalized linear regression.
  * 一个演示广义线性回归的例子
 * Run with
 * {{{
 * bin/run-example ml.GeneralizedLinearRegressionExample
 * }}}
 */

object GeneralizedLinearRegressionExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("GeneralizedLinearRegressionExample")
      .getOrCreate()

    // $example on$
    // Load training data
    //加载训练数据
    val dataset = spark.read.format("libsvm")
      .load("data/mllib/sample_linear_regression_data.txt")

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian") //binomial 二项式
      .setLink("identity")//logit
      .setMaxIter(10)//
      .setRegParam(0.3)//

    // Fit the model
    val model = glr.fit(dataset)

    // Print the coefficients and intercept for generalized linear regression model
    //打印广义线性回归模型的系数和截距
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    //总结训练集上的模型并打印出一些指标
    val summary = model.summary
    //系数标准错误
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    //分散
    println(s"Dispersion: ${summary.dispersion}")
    //零偏离
    println(s"Null Deviance: ${summary.nullDeviance}")
    //剩余自由度
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    //剩余自由度
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals() //.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
