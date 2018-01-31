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

package org.apache.spark.ml.tree.impl

import scala.collection.JavaConverters._

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tree._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

private[ml] object TreeTests extends SparkFunSuite {

  /**
   * Convert the given data to a DataFrame, and set the features and label metadata.
    * 将给定的数据转换为DataFrame,并设置要素和标签元数据
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
    *              数据集,分类特征和标签必须已经具有基于0的索引,这必须是非空的
   *              This must be non-empty.
   * @param categoricalFeatures  Map: categorical feature index to number of distinct values
    *                             Map：分类特征索引到不同值的数量
   * @param numClasses  Number of classes label can take.  If 0, mark as continuous.
    *                    类标签的数量可以采取。 如果为0，则标记为连续。
   * @return DataFrame with metadata
   */
  def setMetadata(
      data: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): DataFrame = {
    val spark = SparkSession.builder()
      .sparkContext(data.sparkContext)
      .getOrCreate()
    import spark.implicits._

    val df = data.toDF()
    val numFeatures = data.first().features.size
    val featuresAttributes = Range(0, numFeatures).map { feature =>
      if (categoricalFeatures.contains(feature)) {
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(categoricalFeatures(feature))
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }.toArray
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    val labelAttribute = if (numClasses == 0) {
      NumericAttribute.defaultAttr.withName("label")
    } else {
      NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    df.select(df("features").as("features", featuresMetadata),
      df("label").as("label", labelMetadata))
  }

  /**
   * Java-friendly version of `setMetadata()`
    * 适用于Java的`setMetadata（）`版本
   */
  def setMetadata(
      data: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): DataFrame = {
    setMetadata(data.rdd, categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numClasses)
  }

  /**
   * Set label metadata (particularly the number of classes) on a DataFrame.
    * 在DataFrame上设置标签元数据（特别是类的数量）
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
    *              数据集,分类特征和标签必须已经具有基于0的索引
   *              This must be non-empty.
   * @param numClasses  Number of classes label can take. If 0, mark as continuous.
    *                    类标签的数量可以采取,如果为0,则标记为连续
   * @param labelColName  Name of the label column on which to set the metadata.
    *                      要在其上设置元数据的标签列的名称
   * @param featuresColName  Name of the features column
   * @return DataFrame with metadata
   */
  def setMetadata(
      data: DataFrame,
      numClasses: Int,
      labelColName: String,
      featuresColName: String): DataFrame = {
    val labelAttribute = if (numClasses == 0) {
      NumericAttribute.defaultAttr.withName(labelColName)
    } else {
      NominalAttribute.defaultAttr.withName(labelColName).withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    data.select(data(featuresColName), data(labelColName).as(labelColName, labelMetadata))
  }

  /**
   * Check if the two trees are exactly the same.
    * 检查两棵树是否完全一样
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
    *       注意：我不愿意重写Node.equals，因为如果用户出现错误，例如创建节点循环，可能会导致问题。
   * If the trees are not equal, this prints the two trees and throws an exception.
    * 如果树不相等,则打印两棵树并抛出异常
   */
  def checkEqual(a: DecisionTreeModel, b: DecisionTreeModel): Unit = {
    try {
      checkEqual(a.rootNode, b.rootNode)
    } catch {
      case ex: Exception =>
        throw new AssertionError("checkEqual failed since the two trees were not identical.\n" +
          "TREE A:\n" + a.toDebugString + "\n" +
          "TREE B:\n" + b.toDebugString + "\n", ex)
    }
  }

  /**
   * Return true iff the two nodes and their descendants are exactly the same.
    * 如果两个节点及其后代完全相同,则返回true
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
    *       注意：我不愿意重写Node.equals,因为如果用户出现错误,例如创建节点循环,可能会导致问题
   */
  private def checkEqual(a: Node, b: Node): Unit = {
    assert(a.prediction === b.prediction)
    assert(a.impurity === b.impurity)
    (a, b) match {
      case (aye: InternalNode, bee: InternalNode) =>
        assert(aye.split === bee.split)
        checkEqual(aye.leftChild, bee.leftChild)
        checkEqual(aye.rightChild, bee.rightChild)
      case (aye: LeafNode, bee: LeafNode) => // do nothing
      case _ =>
        throw new AssertionError("Found mismatched nodes")
    }
  }

  /**
   * Check if the two models are exactly the same.
    * 检查两个模型是否完全相同
   * If the models are not equal, this throws an exception.
    * 如果模型不相等,则会引发异常
   */
  def checkEqual[M <: DecisionTreeModel](a: TreeEnsembleModel[M], b: TreeEnsembleModel[M]): Unit = {
    try {
      a.trees.zip(b.trees).foreach { case (treeA, treeB) =>
        TreeTests.checkEqual(treeA, treeB)
      }
      assert(a.treeWeights === b.treeWeights)
    } catch {
      case ex: Exception => throw new AssertionError(
        "checkEqual failed since the two tree ensembles were not identical")
    }
  }

  /**
   * Helper method for constructing a tree for testing.
    * Helper方法构建测试树
   * Given left, right children, construct a parent node.
    * 给定左侧,右侧的孩子,构建一个父节点
   * @param split  Split for parent node
   * @return  Parent node with children attached
   */
  def buildParentNode(left: Node, right: Node, split: Split): Node = {
    val leftImp = left.impurityStats
    val rightImp = right.impurityStats
    val parentImp = leftImp.copy.add(rightImp)
    val leftWeight = leftImp.count / parentImp.count.toDouble
    val rightWeight = rightImp.count / parentImp.count.toDouble
    val gain = parentImp.calculate() -
      (leftWeight * leftImp.calculate() + rightWeight * rightImp.calculate())
    val pred = parentImp.predict
    new InternalNode(pred, parentImp.calculate(), gain, left, right, split, parentImp)
  }

  /**
   * Create some toy data for testing feature importances.
    * 创建一些玩具数据来测试功能重要性
   */
  def featureImportanceData(sc: SparkContext): RDD[LabeledPoint] = sc.parallelize(Seq(
    new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 1)),
    new LabeledPoint(1, Vectors.dense(1, 1, 0, 1, 0)),
    new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0)),
    new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 0)),
    new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0))
  ))

  /**
   * Create some toy data for testing correctness of variance.
    * 创建一些玩具数据来测试方差的正确性
   */
  def varianceData(sc: SparkContext): RDD[LabeledPoint] = sc.parallelize(Seq(
    new LabeledPoint(1.0, Vectors.dense(Array(0.0))),
    new LabeledPoint(2.0, Vectors.dense(Array(1.0))),
    new LabeledPoint(3.0, Vectors.dense(Array(2.0))),
    new LabeledPoint(10.0, Vectors.dense(Array(3.0))),
    new LabeledPoint(12.0, Vectors.dense(Array(4.0))),
    new LabeledPoint(14.0, Vectors.dense(Array(5.0)))
  ))

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
    * 从所有参数映射到与默认设置不同的有效设置
   * This is useful for tests which need to exercise all Params, such as save/load.
    * 这对于需要执行所有参数的测试非常有用,例如保存/加载
   * This excludes input columns to simplify some tests.
    * 这排除了输入列以简化一些测试
   *
   * This set of Params is for all Decision Tree-based models.
    * 这组Params适用于所有基于决策树的模型
   */
  val allParamSettings: Map[String, Any] = Map(
    "checkpointInterval" -> 7,
    "seed" -> 543L,
    "maxDepth" -> 2,
    "maxBins" -> 20,
    "minInstancesPerNode" -> 2,
    "minInfoGain" -> 1e-14,
    "maxMemoryInMB" -> 257,
    "cacheNodeIds" -> true
  )

  /** Data for tree read/write tests which produces a non-trivial tree.
    * 数据为树读/写测试产生一个非平凡的树*/
  def getTreeReadWriteData(sc: SparkContext): RDD[LabeledPoint] = {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 2.0)),
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 2.0)))
    sc.parallelize(arr)
  }
}
