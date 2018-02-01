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

import scala.collection.mutable
import scala.language.reflectiveCalls

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.DataFrame


/**
 * An example runner for decision trees. Run with
 * 随机森林(Random Forests)其实就是多个决策树,每个决策树有一个权重,对未知数据进行预测时,
 * 会用多个决策树分别预测一个值,然后考虑树的权重,将这多个预测值综合起来,
 * 对于分类问题,采用多数表决,对于回归问题,直接求平均。
 * {{{
 * ./bin/run-example ml.RandomForestExample [options]
 * }}}
 * Decision Trees and ensembles can take a large amount of memory.  If the run-example command
 * above fails, try running via spark-submit and specifying the amount of memory as at least 1g.
 * For local mode, run
 * {{{
 * ./bin/spark-submit --class org.apache.spark.examples.ml.RandomForestExample --driver-memory 1g
 *   [examples JAR path] [options]
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object RandomForestExample {

  case class Params(
      input: String = "../data/mllib/sample_multiclass_classification_data.txt",
      testInput: String = "",
      /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
      dataFormat: String = "libsvm",
      algo: String = "classification",//分类
      maxDepth: Int = 5,//树的最大深度,为了防止过拟合,设定划分的终止条件
      maxBins: Int = 32,//离散连续性变量时最大的分箱数,默认是 32
      minInstancesPerNode: Int = 1,//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
      minInfoGain: Double = 0.0,//分裂节点时所需最小信息增益
      numTrees: Int = 10,//随机森林需要训练的树的个数,默认值是 20
      featureSubsetStrategy: String = "auto",
      fracTest: Double = 0.2,
      cacheNodeIds: Boolean = false,
      checkpointDir: Option[String] = None,
      //设置检查点间隔(>=1),或不设置检查点(-1)
      checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("RandomForestExample") {
      head("RandomForestExample: an example random forest app.")
      opt[String]("algo")
        .text(s"algorithm (classification, regression), default: ${defaultParams.algo}")
        .action((x, c) => c.copy(algo = x))
      opt[Int]("maxDepth")//树的最大深度,为了防止过拟合,设定划分的终止条件
        .text(s"max depth of the tree, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"max number of bins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[Int]("minInstancesPerNode")//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
        .text(s"min number of instances required at child nodes to create the parent split," +
        s" default: ${defaultParams.minInstancesPerNode}")//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
        .action((x, c) => c.copy(minInstancesPerNode = x))//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
      opt[Double]("minInfoGain")//分裂节点时所需最小信息增益
        .text(s"min info gain required to create a split, default: ${defaultParams.minInfoGain}")
        .action((x, c) => c.copy(minInfoGain = x))
      opt[Int]("numTrees")
        .text(s"number of trees in ensemble, default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(numTrees = x))
      opt[String]("featureSubsetStrategy")
        .text(s"number of features to use per node (supported:" +
        s" ${RandomForestClassifier.supportedFeatureSubsetStrategies.mkString(",")})," +
        s" default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(featureSubsetStrategy = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
        s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[Boolean]("cacheNodeIds")
        .text(s"whether to use node Id cache during training, " +
        s"default: ${defaultParams.cacheNodeIds}")
        .action((x, c) => c.copy(cacheNodeIds = x))
      opt[String]("checkpointDir")
        .text(s"checkpoint directory where intermediate node Id caches will be stored, " +
        s"default: ${
          defaultParams.checkpointDir match {
            case Some(strVal) => strVal
            case None => "None"
          }
        }")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"how often to checkpoint the node Id cache, " +
        s"default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
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
        //.required()
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
    val conf = new SparkConf().setAppName(s"RandomForestExample with $params").setMaster("local[*]")
    val sc = new SparkContext(conf)
    params.checkpointDir.foreach(sc.setCheckpointDir)
    val algo = params.algo.toLowerCase

    println(s"RandomForestExample with parameters:\n$params")

    // Load training and test data and cache it.加载训练和测试数据并将其缓存
    val (training: DataFrame, test: DataFrame) = DecisionTreeExample.loadDatasets(params.input,
      params.dataFormat, params.testInput, algo, params.fracTest)

    // Set up Pipeline 建立管道
     //将特征转换,特征聚合,模型等组成一个管道,并调用它的fit方法拟合出模型
     //一个 Pipeline 在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val stages = new mutable.ArrayBuffer[PipelineStage]()
    // (1) For classification, re-index classes.对于分类,重新索引类
    val labelColName = if (algo == "classification") "indexedLabel" else "label"
    if (algo == "classification") {
      val labelIndexer = new StringIndexer()
        .setInputCol("labelString")
        .setOutputCol(labelColName)
      stages += labelIndexer
    }
    // (2) Identify categorical features using VectorIndexer.确定使用vectorindexer分类特征
    //     Features with more than maxCategories values will be treated as continuous.
    //超过maxcategories值将被视为连续的特点
    //VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featuresIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(10)
    stages += featuresIndexer 
    // (3) Learn Random Forest 学习随机森林
    val dt = algo match {
      case "classification" =>
        new RandomForestClassifier()
          .setFeaturesCol("indexedFeatures")//训练数据集 DataFrame 中存储特征数据的列名
          .setLabelCol(labelColName)//标签列的名称
          .setMaxDepth(params.maxDepth)//树的最大深度,为了防止过拟合,设定划分的终止条件
          .setMaxBins(params.maxBins)//离散连续性变量时最大的分箱数,默认是 32
          .setMinInstancesPerNode(params.minInstancesPerNode)//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
          .setMinInfoGain(params.minInfoGain)//分裂节点时所需最小信息增益
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)//
          .setFeatureSubsetStrategy(params.featureSubsetStrategy)
          .setNumTrees(params.numTrees)//随机森林需要训练的树的个数,默认值是 20
      case "regression" =>
        new RandomForestRegressor()
          .setFeaturesCol("indexedFeatures")//训练数据集 DataFrame 中存储特征数据的列名
          .setLabelCol(labelColName)//标签列的名称
          .setMaxDepth(params.maxDepth)//树的最大深度,为了防止过拟合,设定划分的终止条件
          .setMaxBins(params.maxBins)//离散连续性变量时最大的分箱数,默认是 32
          .setMinInstancesPerNode(params.minInstancesPerNode)//切分后每个子节点至少包含的样本实例数,否则停止切分,于终止迭代计算
          .setMinInfoGain(params.minInfoGain)//分裂节点时所需最小信息增益
          .setCacheNodeIds(params.cacheNodeIds)
          .setCheckpointInterval(params.checkpointInterval)//设置检查点间隔(>=1),或不设置检查点(-1)
          .setFeatureSubsetStrategy(params.featureSubsetStrategy)
          .setNumTrees(params.numTrees)//随机森林需要训练的树的个数,默认值是 20
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }
    stages += dt
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline 安装管道
    //系统计时器的当前值,以毫微秒为单位
    val startTime = System.nanoTime()
    //fit()方法将DataFrame转化为一个Transformer的算法
    val pipelineModel = pipeline.fit(training)
    //1e9就为1*(10的九次方),也就是十亿
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Get the trained Random Forest from the fitted PipelineModel
    //从安装管道模型,得到训练随机森林
    algo match {
      case "classification" =>
        val rfModel = pipelineModel.stages.last.asInstanceOf[RandomForestClassificationModel]
        if (rfModel.totalNumNodes < 30) {
          println(rfModel.toDebugString) // Print full model.
        } else {
          println(rfModel) // Print model summary.
        }
      case "regression" =>
        val rfModel = pipelineModel.stages.last.asInstanceOf[RandomForestRegressionModel]
        if (rfModel.totalNumNodes < 30) {
          println(rfModel.toDebugString) // Print full model.
        } else {
          println(rfModel) // Print model summary.
        }
      case _ => throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }

    // Evaluate model on training, test data
    //训练评估模型,测试数据
    algo match {
      case "classification" => //分类
        println("Training data results:")
        DecisionTreeExample.evaluateClassificationModel(pipelineModel, training, labelColName)
        println("Test data results:")
        DecisionTreeExample.evaluateClassificationModel(pipelineModel, test, labelColName)
      case "regression" => //回归
        println("Training data results:")
        DecisionTreeExample.evaluateRegressionModel(pipelineModel, training, labelColName)
        println("Test data results:")
        DecisionTreeExample.evaluateRegressionModel(pipelineModel, test, labelColName)
      case _ =>
        throw new IllegalArgumentException("Algo ${params.algo} not supported.")
    }

    sc.stop()
  }
}
// scalastyle:on println
