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

package org.apache.spark.ml.util

import java.io.{File, IOException}

import org.scalatest.Suite

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

trait DefaultReadWriteTest extends TempDirectory { self: Suite =>

  /**
   * Checks "overwrite" option and params.
    * 检查“覆盖”选项和参数
   * This saves to and loads from [[tempDir]], but creates a subdirectory with a random name
   * in order to avoid conflicts from multiple calls to this method.
   * 这将保存到[[tempDir]]并加载,但会创建一个随机名称的子目录,以避免多次调用此方法的冲突。
    *
   * @param instance ML instance to test saving/loading ML实例来测试保存/加载
   * @param testParams  If true, then test values of Params.  Otherwise, just test overwrite option.
    *                    如果为true，则测试Params的值。 否则，只需测试覆盖选项。
   * @tparam T ML instance type ML实例类型
   * @return  Instance loaded from file 从文件加载的实例
   */
  def testDefaultReadWrite[T <: Params with MLWritable](
      instance: T,
      testParams: Boolean = true): T = {
    val uid = instance.uid
    val subdirName = Identifiable.randomUID("test")

    val subdir = new File(tempDir, subdirName)
    val path = new File(subdir, uid).getPath

    instance.save(path)
    intercept[IOException] {
      instance.save(path)
    }
    //MLWritable
    instance.write.overwrite().save(path)
    val loader = instance.getClass.getMethod("read").invoke(null).asInstanceOf[MLReader[T]]
    val newInstance = loader.load(path)
    assert(newInstance.uid === instance.uid)
    if (testParams) {
      instance.params.foreach { p =>
        if (instance.isDefined(p)) {
          (instance.getOrDefault(p), newInstance.getOrDefault(p)) match {
            case (Array(values), Array(newValues)) =>
              assert(values === newValues, s"Values do not match on param ${p.name}.")
            case (value, newValue) =>
              assert(value === newValue, s"Values do not match on param ${p.name}.")
          }
        } else {
          assert(!newInstance.isDefined(p), s"Param ${p.name} shouldn't be defined.")
        }
      }
    }

    val load = instance.getClass.getMethod("load", classOf[String])
    val another = load.invoke(instance, path).asInstanceOf[T]
    assert(another.uid === instance.uid)
    another
  }

  /**
   * Default test for Estimator, Model pairs:
    * Estimator,Model对的默认测试：
   *  - Explicitly set Params, and train model 显式设置参数,并训练模型
   *  - Test save/load using `testDefaultReadWrite` on Estimator and Model
    * -在Estimator和Model上使用`testDefaultReadWrite`测试保存/加载
   *  - Check Params on Estimator and Model 检查参数估计和模型
   *  - Compare model data 比较模型数据
   *
   * This requires that `Model`'s `Param`s should be a subset of `Estimator`'s `Param`s.
   * 这就要求`Model`的`Param`应该是`Estimator`的`Param`的一个子集。
   * @param estimator  Estimator to test 估算器进行测试
   * @param dataset  Dataset to pass to `Estimator.fit()`
   * @param testEstimatorParams  Set of `Param` values to set in estimator 在参数估计器中设置“参数”值
   * @param testModelParams Set of `Param` values to set in model 在模型中设置“参数”值
   * @param checkModelData  Method which takes the original and loaded `Model` and compares their
   *                        data.  This method does not need to check `Param` values.
    *                        采取原始和加载“模型”,并比较他们的数据的方法,这个方法不需要检查`Param`值
   * @tparam E  Type of `Estimator` “估算器”的类型
   * @tparam M  Type of `Model` produced by estimator 估计器产生的“模型”类型
   */
  def testEstimatorAndModelReadWrite[
    E <: Estimator[M] with MLWritable, M <: Model[M] with MLWritable](
      estimator: E,
      dataset: Dataset[_],
      testEstimatorParams: Map[String, Any],
      testModelParams: Map[String, Any],
      checkModelData: (M, M) => Unit): Unit = {
    // Set some Params to make sure set Params are serialized.
    //设置一些参数,以确保设置参数序列化
    testEstimatorParams.foreach { case (p, v) =>
      estimator.set(estimator.getParam(p), v)
    }
    val model = estimator.fit(dataset)

    // Test Estimator save/load
    //测试估算器保存/加载
    val estimator2 = testDefaultReadWrite(estimator)
    testEstimatorParams.foreach { case (p, v) =>
      val param = estimator.getParam(p)
      assert(estimator.get(param).get === estimator2.get(param).get)
    }

    // Test Model save/load
    //测试模型保存/加载
    val model2 = testDefaultReadWrite(model)
    testModelParams.foreach { case (p, v) =>
      val param = model.getParam(p)
      assert(model.get(param).get === model2.get(param).get)
    }

    checkModelData(model, model2)
  }
}

class MyParams(override val uid: String) extends Params with MLWritable {

  final val intParamWithDefault: IntParam = new IntParam(this, "intParamWithDefault", "doc")
  final val intParam: IntParam = new IntParam(this, "intParam", "doc")
  final val floatParam: FloatParam = new FloatParam(this, "floatParam", "doc")
  final val doubleParam: DoubleParam = new DoubleParam(this, "doubleParam", "doc")
  final val longParam: LongParam = new LongParam(this, "longParam", "doc")
  final val stringParam: Param[String] = new Param[String](this, "stringParam", "doc")
  final val intArrayParam: IntArrayParam = new IntArrayParam(this, "intArrayParam", "doc")
  final val doubleArrayParam: DoubleArrayParam =
    new DoubleArrayParam(this, "doubleArrayParam", "doc")
  final val stringArrayParam: StringArrayParam =
    new StringArrayParam(this, "stringArrayParam", "doc")

  setDefault(intParamWithDefault -> 0)
  set(intParam -> 1)
  set(floatParam -> 2.0f)
  set(doubleParam -> 3.0)
  set(longParam -> 4L)
  set(stringParam -> "5")
  set(intArrayParam -> Array(6, 7))
  set(doubleArrayParam -> Array(8.0, 9.0))
  set(stringArrayParam -> Array("10", "11"))

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  override def write: MLWriter = new DefaultParamsWriter(this)
}

object MyParams extends MLReadable[MyParams] {

  override def read: MLReader[MyParams] = new DefaultParamsReader[MyParams]

  override def load(path: String): MyParams = super.load(path)
}

class DefaultReadWriteSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  test("default read/write") {
    val myParams = new MyParams("my_params")
    testDefaultReadWrite(myParams)
  }
}
