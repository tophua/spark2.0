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

import org.apache.commons.math3.linear._

/**
 * Alternating least squares matrix factorization.
 * 交替最小二乘矩阵分解
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.recommendation.ALS.
  * 这是一个学习如何使用Spark的例子实现,更多的常规使用
 */
object LocalALS {

  // Parameters set through command line arguments
  //通过命令行参数设置的参数
  var M = 0 // Number of movies 电影数
  var U = 0 // Number of users  用户数
  var F = 0 // Number of features 特征数
  var ITERATIONS = 0 //迭代次数
  val LAMBDA = 0.01 // Regularization coefficient 正则化系数

  def generateR(): RealMatrix = {
    val mh = randomMatrix(M, F)//10行,5列
    val uh = randomMatrix(U, F)
    mh.multiply(uh.transpose()) //transpose转置矩阵
  }
  //rmse均方根误差说明样本的离散程度
  def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
    val r = new Array2DRowRealMatrix(M, U)
    for (i <- 0 until M; j <- 0 until U) {
      r.setEntry(i, j, ms(i).dotProduct(us(j)))
    }
    val diffs = r.subtract(targetR)
    var sumSqs = 0.0
    for (i <- 0 until M; j <- 0 until U) {
      val diff = diffs.getEntry(i, j)
      sumSqs += diff * diff
    }
    math.sqrt(sumSqs / (M.toDouble * U.toDouble))
  }

  def updateMovie(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix) : RealVector = {
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each user that rated the movie
    //为每一个用户评价的电影
    for (j <- 0 until U) {
      val u = us(j)
      // Add u * u^t to XtX
      XtX = XtX.add(u.outerProduct(u))
      // Add u * rating to Xty
      Xty = Xty.add(u.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefficients to diagonal terms
    //向对角项的正则化系数
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    // Solve it with Cholesky
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def updateUser(j: Int, u: RealVector, ms: Array[RealVector], R: RealMatrix) : RealVector = {
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each movie that the user rated
    //对于每一部电影的用户评分
    for (i <- 0 until M) {
      val m = ms(i)
      // Add m * m^t to XtX
      XtX = XtX.add(m.outerProduct(m))
      // Add m * rating to Xty
      Xty = Xty.add(m.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefficients to diagonal terms
    //将正则化系数添加到对角项
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * M)
    }
    // Solve it with Cholesky
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of ALS and is given as an example!
        |Please use org.apache.spark.ml.recommendation.ALS
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    args match {
      case Array(m, u, f, iters) =>
        M = m.toInt
        U = u.toInt
        F = f.toInt
        ITERATIONS = iters.toInt
      case _ =>
        System.err.println("Usage: LocalALS <M> <U> <F> <iters>")
        System.exit(1)
    }

    showWarning()

    println(s"Running with M=$M, U=$U, F=$F, iters=$ITERATIONS")

    val R = generateR()

    // Initialize m and u randomly
    var ms = Array.fill(M)(randomVector(F))
    var us = Array.fill(U)(randomVector(F))

    // Iteratively update movies then users
    //迭代更新电影然后用户
    for (iter <- 1 to ITERATIONS) { //迭代次数计算
      println(s"Iteration $iter:")
      ms = (0 until M).map(i => updateMovie(i, ms(i), us, R)).toArray
      us = (0 until U).map(j => updateUser(j, us(j), ms, R)).toArray
      //rmse均方根误差说明样本的离散程度
      println("RMSE = " + rmse(R, ms, us))
      println()
    }
  }

  private def randomVector(n: Int): RealVector =
    new ArrayRealVector(Array.fill(n)(math.random))

  private def randomMatrix(rows: Int, cols: Int): RealMatrix =
    new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}
// scalastyle:on println
