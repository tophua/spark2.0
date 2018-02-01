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
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating ALS.
 * Run with
 * {{{
 * bin/run-example ml.ALSExample
  * 从MovieLens dataset读入评分数据,每一行包括用户、电影、评分以及时间戳
 * }}}
 */
object ALSExample {

  // $example on$
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  // $example off$

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val ratings = spark.read.textFile("data/mllib/als/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5) //设置最大迭代数
      .setRegParam(0.01) //正则化参数
      .setUserCol("userId") //设置用户列名
      .setItemCol("movieId") //设置商品编号列名
      .setRatingCol("rating")  //设置评分列名
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    //通过计算测试数据上的RMSE来评估模型
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    //请注意,我们将冷启动策略设置为“drop”以确保我们不会获得NaN评估指标
    model.setColdStartStrategy("drop")
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predictions = model.transform(test)
    //通过预测评分的均方根误差来评价推荐模型
    val evaluator = new RegressionEvaluator()
      //rmse均方根误差说明样本的离散程度
      .setMetricName("rmse")
      //标签列的名称
      .setLabelCol("rating")
      //算法预测结果的存储列的名称, 默认是”prediction”
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    //rmse均方根误差说明样本的离散程度
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    //为每个用户生成前10名电影推荐
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    //为每部电影生成前10名用户推荐
    val movieRecs = model.recommendForAllItems(10)
    // $example off$
    userRecs.show()
    movieRecs.show()

    spark.stop()
  }
}
// scalastyle:on println

