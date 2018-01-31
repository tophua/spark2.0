package org.apache.spark.examples.ml

import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row

/**
  * Created by liush on 17-12-19 
  */
object MaxAbsScalerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {


    val data = Array(
      Vectors.dense(1, 0, 100),
      Vectors.dense(2, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(-2, -100)),
      Vectors.sparse(3, Array(0), Array(-1.5)))

    val expected: Array[Vector] = Array(
      Vectors.dense(0.5, 0, 1),
      Vectors.dense(1, 0, 0),
      Vectors.sparse(3, Array(0, 2), Array(-1, -1)),
      Vectors.sparse(3, Array(0), Array(-0.75)))

   // val sqlCon = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    import spark.implicits._
    val df = data.zip(expected).toSeq.toDF("features", "expected")
    //MaxAbsScaler将每一维的特征变换到[-1, 1]闭区间上,通过除以每一维特征上的最大的绝对值,它不会平移整个分布,也不会破坏原来每一个特征向量的稀疏性
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaled")

    val model = scaler.fit(df)
    /**
    +--------------------+--------------------+--------------------+
      |            features|            expected|              scaled|
      +--------------------+--------------------+--------------------+
      |     [1.0,0.0,100.0]|       [0.5,0.0,1.0]|       [0.5,0.0,1.0]|
      |       [2.0,0.0,0.0]|       [1.0,0.0,0.0]|       [1.0,0.0,0.0]|
      |(3,[0,2],[-2.0,-1...|(3,[0,2],[-1.0,-1...|(3,[0,2],[-1.0,-1...|
      |      (3,[0],[-1.5])|     (3,[0],[-0.75])|     (3,[0],[-0.75])|
      +--------------------+--------------------+--------------------+
      **/
    model.transform(df).show()
    model.transform(df).select("expected", "scaled").collect()
      .foreach { case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1.equals(vector2), s"MaxAbsScaler ut error: $vector2 should be $vector1")
      }
  }

}
