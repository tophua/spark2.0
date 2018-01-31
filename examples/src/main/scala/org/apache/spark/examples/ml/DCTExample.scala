package org.apache.spark.examples.ml

/**
  * Created by liush on 17-12-19
  * Discrete Cosine Transform (DCT-离散余弦变换)
  */
object DCTExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    import org.apache.spark.ml.feature.DCT
    import org.apache.spark.ml.linalg.Vectors

    val data = Seq(
      Vectors.dense(0.0, 1.0, -2.0, 3.0),
      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
      Vectors.dense(14.0, -2.0, -5.0, 1.0))

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    /**
      * 离散余弦变换(Discrete Cosine Transform)是将时域的N维实数序列转换成频域的N维实数序列的过程(有点类似离散傅里叶变换)
      * (ML中的)DCT类提供了离散余弦变换DCT-II的功能,将离散余弦变换后结果乘以12√12得到一个与时域矩阵长度一致的矩阵,输入序列与输出之间是一一对应的。
      */
    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDf = dct.transform(df)
    dctDf.select("featuresDCT").show(false)
  }
}
