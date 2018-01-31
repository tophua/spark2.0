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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{IDFModel => OldIDFModel}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
/**
  * HashTF从一个文档中计算出给定大小的词频向量。为了将词和向量顺序对应起来,所以使用了哈希。
  * HashingTF使用每个单词对所需向量的长度S取模得出的哈希值,把所有单词映射到一个0到S-1之间的数字上。
  * 由此可以保证生成一个S维的向量。随后当构建好词频向量后,使用IDF来计算逆文档频率,然后将它们与词频相乘计算TF-IDF
  */
class IDFSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  def scaleDataWithIDF(dataSet: Array[Vector], model: Vector): Array[Vector] = {
    dataSet.map {
      case data: DenseVector =>
        val res = data.toArray.zip(model.toArray).map { case (x, y) => x * y }
        Vectors.dense(res)
      case data: SparseVector =>
        val res = data.indices.zip(data.values).map { case (id, value) =>
          (id, value * model(id))
        }
        Vectors.sparse(data.size, res)
    }
  }

  test("params") {
    ParamsSuite.checkParams(new IDF)
    val model = new IDFModel("idf", new OldIDFModel(Vectors.dense(1.0)))
    ParamsSuite.checkParams(model)
  }
  //默认参数计算IDF
  test("compute IDF with default parameter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((numOfData + 1.0) / (x + 1.0))
    })
    val expected = scaleDataWithIDF(data, idf)
    //计算逆词频 idf
    val df = data.zip(expected).toSeq.toDF("features", "expected")

    val idfEst = new IDF()
      .setInputCol("features")
      .setOutputCol("idfValue")
    val idfModel = idfEst.fit(df)

    MLTestingUtils.checkCopyAndUids(idfEst, idfModel)

    idfModel.transform(df).select("idfValue", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x ~== y absTol 1e-5, "Transformed vector is different with expected vector.")
    }
  }
  //设置IDF计算
  test("compute IDF with setter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) math.log((numOfData + 1.0) / (x + 1.0)) else 0
    })
    val expected = scaleDataWithIDF(data, idf)

    val df = data.zip(expected).toSeq.toDF("features", "expected")

    val idfModel = new IDF()
      .setInputCol("features")
      .setOutputCol("idfValue")
      .setMinDocFreq(1)
      .fit(df)

    idfModel.transform(df).select("idfValue", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x ~== y absTol 1e-5, "Transformed vector is different with expected vector.")
    }
  }

  test("IDF read/write") {
    val t = new IDF()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMinDocFreq(5)
    testDefaultReadWrite(t)
  }

  test("IDFModel read/write") {
    val instance = new IDFModel("myIDFModel", new OldIDFModel(Vectors.dense(1.0, 2.0)))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.idf === instance.idf)
  }
}
