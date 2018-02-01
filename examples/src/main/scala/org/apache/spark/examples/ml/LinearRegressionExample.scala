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

import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

import scala.language.reflectiveCalls

/**
 * An example runner for linear regression with elastic-net (mixing L1/L2) regularization.
 * Run with
 * {{{
 * bin/run-example ml.LinearRegressionExample [options]
 * }}}
 * A synthetic dataset can be found at `data/mllib/sample_linear_regression_data.txt` which can be
 * trained by
 * {{{
 * bin/run-example ml.LinearRegressionExample --regParam 0.15 --elasticNetParam 1.0 \
 *   data/mllib/sample_linear_regression_data.txt
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LinearRegressionExample {

  case class Params(
      input: String = "../data/mllib/sample_libsvm_data.txt",
      testInput: String = "",
      /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
      dataFormat: String = "libsvm",
      regParam: Double = 0.0,
      elasticNetParam: Double = 0.0,//ElasticNetParam=0.0为L2正则化 1.0为L1正则化
      maxIter: Int = 100,
      tol: Double = 1E-6,//迭代算法的收敛性
      fracTest: Double = 0.2) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LinearRegressionExample") {
      head("LinearRegressionExample: an example Linear Regression with Elastic-Net app.")
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Double]("elasticNetParam")//ElasticNetParam=0.0为L2正则化 1.0为L1正则化
        .text(s"ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty. " +
        s"For alpha = 1, it is an L1 penalty. For 0 < alpha < 1, the penalty is a combination of " +
        s"L1 and L2, default: ${defaultParams.elasticNetParam}")
        .action((x, c) => c.copy(elasticNetParam = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("tol")//迭代算法的收敛性
        .text(s"the convergence tolerance of iterations, Smaller value will lead " +
        s"to higher accuracy with the cost of more iterations, default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
        s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("testInput")
        .text(s"input path to test dataset.  If given, option fracTest is ignored." +
        s" default: ${defaultParams.testInput}")
        .action((x, c) => c.copy(testInput = x))
      opt[String]("dataFormat")
      /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      /*arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))*/
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val spark = SparkSession
      .builder
      .appName(s"LinearRegressionExample with $params")
      .getOrCreate()

    println(s"LinearRegressionExample with parameters:\n$params")

    // Load training and test data and cache it.
    //加载训练和测试数据并缓存它
    val (training: DataFrame, test: DataFrame) = DecisionTreeExample.loadDatasets(params.input,
      params.dataFormat, params.testInput, "regression", params.fracTest)

    val lir = new LinearRegression()
    //训练数据集DataFrame中存储特征数据的列名
      .setFeaturesCol("features")//特征名
      .setLabelCol("label")//标签列
      .setRegParam(params.regParam)//设置正则化参数
      .setElasticNetParam(params.elasticNetParam)//ElasticNetParam=0.0为L2正则化 1.0为L1正则化
      .setMaxIter(params.maxIter)//设置最大迭代次数
      .setTol(params.tol)//设置迭代的收敛

    // Train the model
    // 系统计时器的当前值,以毫微秒为单位
    val startTime = System.nanoTime()
    //fit()方法将DataFrame转化为一个Transformer的算法
    val lirModel = lir.fit(training)
    //1e9就为1*(10的九次方),也就是十亿
    //经过的时间
    val elapsedTime = (System.nanoTime() - startTime) / 1e9   
    //Training time: 8.57000559 seconds
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for linear regression.
    //Weights: (692,[95,96,97,98,99,100,101,102,119]) Intercept: 0.7644843078616104
    println(s"Weights: ${lirModel.coefficients} Intercept: ${lirModel.intercept}")

    println("Training data results:")
    //Root mean squared error (RMSE): 3.1575761537716335E-4
    DecisionTreeExample.evaluateRegressionModel(lirModel, training, "label")
    println("Test data results:")
    //Root mean squared error (RMSE): 0.5858636434734112
    DecisionTreeExample.evaluateRegressionModel(lirModel, test, "label")

    sc.stop()
  }
}
// scalastyle:on println
