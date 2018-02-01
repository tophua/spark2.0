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
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example for Multilayer Perceptron Classification.
  * 多层感知器分类的一个例子
 */
object MultilayerPerceptronClassifierExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MultilayerPerceptronClassifierExample")
      .getOrCreate()

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    //加载LIBSVM格式的数据作为DataFrame
    val data = spark.read.format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt")

    // Split the data into train and test
    //将数据分解成训练和测试
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    // specify layers for the neural network:
    //为神经网络指定图层：
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    //大小为4（特征）的输入层,大小为5的两个中间体和大小为4（特征）的4个输入层,大小为5和4的两个中间体
    val layers = Array[Int](4, 5, 4, 3)

    // create the trainer and set its parameters
    //创建教练并设置其参数
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)//层规模,包括输入规模以及输出规模
      .setBlockSize(128)//块的大小,以加快计算
      .setSeed(1234L)//随机种子
      .setMaxIter(100)//迭代次数

    // train the model
    //训练模型
    val model = trainer.fit(train)

    // compute accuracy on the test set
    //计算测试集的精度
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    //多分类评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    //准确度 Accuracy: 0.9636363636363636
    println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
