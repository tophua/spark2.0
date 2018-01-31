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

package org.apache.spark.ml.classification

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.tree.{CategoricalSplit, InternalNode, LeafNode}
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree, DecisionTreeSuite => OldDecisionTreeSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class DecisionTreeClassifierSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import DecisionTreeClassifierSuite.compareAPIs
  import testImplicits._

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel0RDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel1RDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var continuousDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassForOrderedFeaturesRDD: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPoints()).map(_.asML)
    orderedLabeledPointsWithLabel0RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()).map(_.asML)
    orderedLabeledPointsWithLabel1RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()).map(_.asML)
    categoricalDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlass()).map(_.asML)
    continuousDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateContinuousDataPointsForMulticlass()).map(_.asML)
    categoricalDataPointsForMulticlassForOrderedFeaturesRDD = sc.parallelize(
      OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures())
      .map(_.asML)
  }

  test("params") {
    ParamsSuite.checkParams(new DecisionTreeClassifier)
    val model = new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 1, 2)
    ParamsSuite.checkParams(model)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////
  //具有有序分类特征的二元分类树桩
  test("Binary classification stump with ordered categorical features") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(2)
      .setMaxBins(100)
      .setSeed(1)
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    val numClasses = 2
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures, numClasses)
  }
  //具有固定标签的二元分类残块0,1用于熵，基尼
  test("Binary classification stump with fixed labels 0,1 for Entropy,Gini") {
    val dt = new DecisionTreeClassifier()
      .setMaxDepth(3)
      .setMaxBins(100)
    val numClasses = 2
    Array(orderedLabeledPointsWithLabel0RDD, orderedLabeledPointsWithLabel1RDD).foreach { rdd =>
      DecisionTreeClassifier.supportedImpurities.foreach { impurity =>
        dt.setImpurity(impurity)
        compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
      }
    }
  }
  //具有3元（无序）分类特征的多类分类树桩
  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 3
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //具有1个连续特征的二进制分类树桩，用于检查偏离1的错误
  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(3.0)))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //具有2个连续特征的二元分类树桩
  test("Binary classification stump with 2 continuous features") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0)))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //具有无序分类特征的多类分类树桩，只有足够的分类
  test("Multiclass classification stump with unordered categorical features," +
    " with just enough bins") {
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(maxBins)
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //具有连续特征的多类分类树桩
  test("Multiclass classification stump with continuous features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //具有连续+无序分类特征的多类分类树桩
  test("Multiclass classification stump with continuous + unordered categorical features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //具有10元（有序）分类特征的多类分类树桩
  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //具有10元（有序）分类特征的多类分类树，只有足够的分类
  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
    " with just enough bins") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(10)
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //拆分必须满足每个节点需求的最小实例
  test("split must satisfy min instances per node requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setMinInstancesPerNode(2)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //不要选择不满足每个节点要求最小实例的拆分
  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    //如果拆分不满足每个节点需求的最小实例数，
    // this split is invalid, even though the information gain of split is large.
    //即使分割的信息增益很大，这个分割也是无效的。
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxBins(2)
      .setMaxDepth(2)
      .setMinInstancesPerNode(2)
    val categoricalFeatures = Map(0 -> 2, 1 -> 2)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }
  //拆分必须满足最低信息增益要求
  test("split must satisfy min info gain requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr)

    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setMinInfoGain(1.0)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }
  //预测和预测可能性
  test("predictRaw and predictProbability") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3

    val newData: DataFrame = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses)
    val newTree = dt.fit(newData)

    MLTestingUtils.checkCopyAndUids(dt, newTree)

    val predictions = newTree.transform(newData)
      .select(newTree.getPredictionCol, newTree.getRawPredictionCol, newTree.getProbabilityCol)
      .collect()

    predictions.foreach { case Row(pred: Double, rawPred: Vector, probPred: Vector) =>
      assert(pred === rawPred.argmax,
        s"Expected prediction $pred but calculated ${rawPred.argmax} from rawPrediction.")
      val sum = rawPred.toArray.sum
      assert(Vectors.dense(rawPred.toArray.map(_ / sum)) === probPred,
        "probability prediction mismatch")
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, DecisionTreeClassificationModel](newTree, newData)
  }
  //训练与1类别的分类功能
  test("training with 1-category categorical feature") {
    val data = sc.parallelize(Seq(
      LabeledPoint(0, Vectors.dense(0, 2, 3)),
      LabeledPoint(1, Vectors.dense(0, 3, 1)),
      LabeledPoint(0, Vectors.dense(0, 2, 2)),
      LabeledPoint(1, Vectors.dense(0, 3, 9)),
      LabeledPoint(0, Vectors.dense(0, 2, 6))
    ))
    val df = TreeTests.setMetadata(data, Map(0 -> 1), 2)
    val dt = new DecisionTreeClassifier().setMaxDepth(3)
    dt.fit(df)
  }
  //使用软预测进行具有有序分类特征的二元分类
  test("Use soft prediction for binary classification with ordered categorical features") {
    // The following dataset is set up such that the best split is {1} vs. {0, 2}.
    // If the hard prediction is used to order the categories, then {0} vs. {1, 2} is chosen.
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)))
    val data = sc.parallelize(arr)
    val df = TreeTests.setMetadata(data, Map(0 -> 3), 2)

    // Must set maxBins s.t. the feature will be treated as an ordered categorical feature.
    //必须设置maxBins s.t. 该功能将被视为有序的分类功能
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(1)
      .setMaxBins(3)
    val model = dt.fit(df)
    model.rootNode match {
      case n: InternalNode =>
        n.split match {
          case s: CategoricalSplit =>
            assert(s.leftCategories === Array(1.0))
          case other =>
            fail(s"All splits should be categorical, but got ${other.getClass.getName}: $other.")
        }
      case other =>
        fail(s"Root node should be an internal node, but got ${other.getClass.getName}: $other.")
    }
  }
  //重要的玩具数据
  test("Feature importance with toy data") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(3)
      .setSeed(123)

    // In this data, feature 1 is very important.
    //在这个数据中，特征1是非常重要的
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val numFeatures = data.first().features.size
    val categoricalFeatures = (0 to numFeatures).map(i => (i, 2)).toMap
    val df = TreeTests.setMetadata(data, categoricalFeatures, 2)

    val model = dt.fit(df)

    val importances = model.featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }
  //应该支持所有的NumericType标签，不支持其他类型
  test("should support all NumericType labels and not support other types") {
    val dt = new DecisionTreeClassifier().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[DecisionTreeClassificationModel, DecisionTreeClassifier](
      dt, spark) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }
  //在元数据中没有numClasses
  test("Fitting without numClasses in metadata") {
    val df: DataFrame = TreeTests.featureImportanceData(sc).toDF()
    val dt = new DecisionTreeClassifier().setMaxDepth(1)
    dt.fit(df)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
        model: DecisionTreeClassificationModel,
        model2: DecisionTreeClassificationModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
      assert(model.numClasses === model2.numClasses)
    }

    val dt = new DecisionTreeClassifier()
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("impurity" -> "entropy")

    // Categorical splits with tree depth 2
    //分类分裂与树深度2
    val categoricalData: DataFrame =
      TreeTests.setMetadata(rdd, Map(0 -> 2, 1 -> 3), numClasses = 2)
    testEstimatorAndModelReadWrite(dt, categoricalData, allParamSettings,
      allParamSettings, checkModelData)

    // Continuous splits with tree depth 2
    //连续分裂与树深度2
    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)
    testEstimatorAndModelReadWrite(dt, continuousData, allParamSettings,
      allParamSettings, checkModelData)

    // Continuous splits with tree depth 0
    //与树深度0连续分割
    testEstimatorAndModelReadWrite(dt, continuousData, allParamSettings ++ Map("maxDepth" -> 0),
      allParamSettings ++ Map("maxDepth" -> 0), checkModelData)
  }
  //杂质计算器生成器在模型读取/写入中不能使用大写的杂质类型基尼
  test("SPARK-20043: " +
       "ImpurityCalculator builder fails for uppercase impurity type Gini in model read/write") {
    val rdd = TreeTests.getTreeReadWriteData(sc)
    val data: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)

    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
    val model = dt.fit(data)

    testDefaultReadWrite(model)
  }
}

private[ml] object DecisionTreeClassifierSuite extends SparkFunSuite {

  /**
   * Train 2 decision trees on the given dataset, one using the old API and one using the new API.
    * 在给定的数据集上训练2个决策树，一个使用旧的API，一个使用新的API。
   * Convert the old tree to the new format, compare them, and fail if they are not exactly equal.
    * 将旧的树转换为新的格式,比较它们,如果它们不完全相同则失败
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      dt: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    val numFeatures = data.first().features.size
    val oldStrategy = dt.getOldStrategy(categoricalFeatures, numClasses)
    val oldTree = OldDecisionTree.train(data.map(OldLabeledPoint.fromML), oldStrategy)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)
    val newTree = dt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldTreeAsNew = DecisionTreeClassificationModel.fromOld(
      oldTree, newTree.parent.asInstanceOf[DecisionTreeClassifier], categoricalFeatures)
    TreeTests.checkEqual(oldTreeAsNew, newTree)
    assert(newTree.numFeatures === numFeatures)
  }
}
