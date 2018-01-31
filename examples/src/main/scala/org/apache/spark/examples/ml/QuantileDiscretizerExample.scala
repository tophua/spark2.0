package org.apache.spark.examples.ml

import org.apache.spark.ml.feature.QuantileDiscretizer

/**
  * Created by liush on 17-12-19 
  */
object QuantileDiscretizerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {


    import spark.implicits._

    val numBuckets = 3  //设置分箱数
    val expectedNumBuckets = 3 //
    val df = spark.sparkContext.parallelize(Array(1.0, 3.0, 2.0, 4.0, 81.0, 6.0, 3.0, 7.0, 2.0, 9.0, 1.0, 3.0))
      .map(Tuple1.apply).toDF("input")
    /**
      * QuantileDiscretizer分位树为数离散化,和Bucketizer(分箱处理)一样也是：将连续数值特征转换为离散类别特征
      */
    val discretizer = new QuantileDiscretizer()
      .setInputCol("input")
      .setOutputCol("result")
      //另外一个参数是精度,如果设置为0,则计算最精确的分位数
     // .setRelativeError(0.1)//设置precision-控制相对误差
      //设置分箱数
      .setNumBuckets(numBuckets)
    val result = discretizer.fit(df).transform(df)

    /**
      * +-----+------+
        |input|result|
        +-----+------+
        |  1.0|   0.0|
        |  3.0|   1.0|
        |  2.0|   1.0|
        |  4.0|   2.0|
        | 81.0|   2.0|
        |  6.0|   2.0|
        |  3.0|   1.0|
        |  7.0|   2.0|
        |  2.0|   1.0|
        |  9.0|   2.0|
        |  1.0|   0.0|
        |  3.0|   1.0|
        +-----+------+
      */
    result.show()
    /*    val observedNumBuckets = result.select("result").distinct.count
  assert(observedNumBuckets == expectedNumBuckets,
       s"Observed number of buckets are not correct." +
         s" Expected $expectedNumBuckets but found $observedNumBuckets")*/
  }

}
