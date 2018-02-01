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
package org.apache.spark.examples

import java.util.Random

import breeze.linalg.{DenseVector, Vector}

/**
 * Logistic regression based classification.
 * 基于逻辑回归的分类
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.classification.LogisticRegression.
  * 这是学习如何使用Spark的示例实现,更传统的使用方法,请参考LogisticRegression
 */
object LocalLR {
  val N = 10000  // Number of data points 数据点
  val D = 10   // Number of dimensions 维度
  val R = 0.7  // Scaling factor 比例因子
  val ITERATIONS = 5 //迭代次数
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D) {rand.nextGaussian + y * R}
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use org.apache.spark.ml.classification.LogisticRegression
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()

    val data = generateData
    // Initialize w to a random value
    //初始化W到一个随机值
    val w = DenseVector.fill(D) {2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {//迭代次数
      println("On iteration " + i)
      val gradient = DenseVector.zeros[Double](D)
      for (p <- data) {//密集向量维度10
        val scale = (1 / (1 + math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
        gradient +=  p.x * scale
      }
      w -= gradient
    }

    println("Final w: " + w)
  }
}
// scalastyle:on println
