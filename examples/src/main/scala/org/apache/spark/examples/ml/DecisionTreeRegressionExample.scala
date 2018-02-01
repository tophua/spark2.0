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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor
// $example off$
import org.apache.spark.sql.SparkSession
/**
  * 回归决策树例子
  */
object DecisionTreeRegressionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DecisionTreeRegressionExample")
      .getOrCreate()

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    //以LIBSVM格式存储数据作为DataFrame
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Automatically identify categorical features, and index them.
    //自动识别分类特征,和索引,在这里,我们训练特征值>4不同值作为连续
    // Here, we treat features with > 4 distinct values as continuous.
    //VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)//最大分类
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    // 将数据分成训练和测试集(30%进行测试)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    // 训练一个决策模型
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      //训练数据集DataFrame中存储特征数据的列名
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and tree in a Pipeline.
    //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    // Train model. This also runs the indexer.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = pipeline.fit(trainingData)

    // Make predictions.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    //回归评估,选择(预测,真实标签)和计算测试错误
    val evaluator = new RegressionEvaluator()
      //标签列的名称
      .setLabelCol("label")
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
      //rmse均方根误差说明样本的离散程度
      .setMetricName("rmse")//均方根误差
    //rmse均方根误差说明样本的离散程度
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
