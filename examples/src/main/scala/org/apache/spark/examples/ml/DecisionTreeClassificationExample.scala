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
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
// $example off$
import org.apache.spark.sql.SparkSession

object DecisionTreeClassificationExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DecisionTreeClassificationExample")
      .getOrCreate()
    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    //加载LIBSVM格式的数据作为DataFrame
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Index labels, adding metadata to the label column.
    //索引标签,将元数据添加到标签列
    // Fit on whole dataset to include all labels in index.
    //适合整个数据集以包含索引中的所有标签
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    //自动识别分类特征,并将其编入索引
    //VectorIndexer是对数据集特征向量中的类别(离散值)特征进行编号
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      //具有4个不同值的特征被为连续
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    //将数据分成训练和测试集（30％用于测试）
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    //训练DecisionTree模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    //将索引标签转换回原始标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
    //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = pipeline.fit(trainingData)

    // Make predictions.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(testData)

    // Select example rows to display.
    //选择要显示的示例行
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    //选择(预测,真实标签)和计算测试错误
    val evaluator = new MulticlassClassificationEvaluator()
      //标签列的名称
      .setLabelCol("indexedLabel")
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
      //F1-Measure是根据准确率Precision和召回率Recall二者给出的一个综合的评价指标
      //测量名称列参数(f1,precision,recall,weightedPrecision,weightedRecall)
      //f1        Test Error = 0.04660856384994316
      //precision Test Error = 0.030303030303030276 准确率
      //recall    Test Error = 0.0
      .setMetricName("accuracy")  //准确度
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
