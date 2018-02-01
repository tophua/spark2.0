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
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example of Multiclass to Binary Reduction with One Vs Rest,
  * 一个Vs休息的多重到二进制减少的例子
 * using Logistic Regression as the base classifier.
  * 使用Logistic回归作为基础分类器
 * Run with
 * {{{
 * ./bin/run-example ml.OneVsRestExample
 * }}}
 */

object OneVsRestExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"OneVsRestExample")
      .getOrCreate()

    // $example on$
    // load data file.
    //加载数据
    val inputData = spark.read.format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt")

    // generate the train/test split.
    //分割数据测试和训练
    val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))

    // instantiate the base classifier
    //实例化基分类器
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

    // instantiate the One Vs Rest Classifier.
    //实例化One Vs Rest分类器
    val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    //训练多类模型
    val ovrModel = ovr.fit(train)

    // score the model on test data.
    //在测试数据上评分模型
    val predictions = ovrModel.transform(test)

    // obtain evaluator.
    //获得评估者
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    // compute the classification error on test data.
    //计算测试数据的分类错误
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1 - accuracy}")
    // $example off$

    spark.stop()
  }

}
// scalastyle:on println
