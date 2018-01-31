package org.apache.spark.examples.ml

/**
  * Created by liush on 17-12-19
  * 索引-字符串变换
  *
  */
object IndexToStringExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    import org.apache.spark.ml.attribute.Attribute
    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")
    //IndexToString将索引化标签还原成原始字符串,一个常用的场景是先通过StringIndexer产生索引化标签，
    //按label出现的频次，转换成0～num numOfLabels-1(分类个数)，频次最高的转换为0
    //1)按照 Label 出现的频次对其进行序列编码,如0,1,2,… Array[String] = Array(a, c, b),a出次3次,c出现2次,b出现1次
    //2)fit()方法将DataFrame转化为一个Transformer的算法
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

    println(s"Transformed string column '${indexer.getInputCol}' " +
      s"to indexed column '${indexer.getOutputCol}'")

    /**
      +---+-------------+----------------+
      | id|categoryIndex|originalCategory|
      +---+-------------+----------------+
      |  0|          0.0|               a|
      |  1|          2.0|               b|
      |  2|          1.0|               c|
      |  3|          0.0|               a|
      |  4|          0.0|               a|
      |  5|          1.0|               c|
      +---+-------------+----------------+
      **/
    indexed.show()
    //获得字段结构
    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println("=========>"+inputColSchema.metadata.json)
    println("====categoryIndex======"+inputColSchema.name+"==="+Attribute.fromStructField(inputColSchema)+"==="+inputColSchema.dataType+"===>>"+inputColSchema.metadata)
    println(s"StringIndexer will store labels in output column metadata: " +
      s"${Attribute.fromStructField(inputColSchema).toString}\n")
    //IndexToString将索引化标签还原成原始字符串
    //按label出现的频次，转换成0～num numOfLabels-1(分类个数)，频次最高的转换为0
    //1)按照 Label 出现的频次对其进行序列编码,如0,1,2,… Array[String] = Array(a, c, b),a出次3次,c出现2次,b出现1次
    //2)fit()方法将DataFrame转化为一个Transformer的算法
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)

    /**
        +---+-------------+----------------+
        | id|categoryIndex|originalCategory|
        +---+-------------+----------------+
        |  0|          0.0|               a|
        |  1|          2.0|               b|
        |  2|          1.0|               c|
        |  3|          0.0|               a|
        |  4|          0.0|               a|
        |  5|          1.0|               c|
        +---+-------------+----------------+
      **/
    converted.show()
    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
      s"column '${converter.getOutputCol}' using labels in metadata")
    converted.select("id", "categoryIndex", "originalCategory").show()
  }
}
