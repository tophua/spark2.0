package org.apache.spark.examples.ml

/**
  * Created by liush on 17-12-19
一个 n-gram是一个长度为n（整数）的字的序列。NGram可以用来将输入特征转换成n-grams。
NGram 的输入为一系列的字符串（例如：Tokenizer分词器的输出）。参数n表示每个n-gram中单词（terms）的数量。
NGram的输出结果是多个n-grams构成的序列，其中，每个n-gram表示被空格分割出来的n个连续的单词。如果输入的字符串少于n个单词，NGram输出为空。
  */
object NGramExample extends SparkCommant{


    def main(args: Array[String]): Unit = {

      import org.apache.spark.ml.feature.NGram

      val wordDataFrame = spark.createDataFrame(Seq(
        (0, Array("Hi", "I", "heard", "about", "Spark")),
        (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
        (2, Array("Logistic", "regression", "models", "are", "neat"))
      )).toDF("id", "words")
      //一个 n-gram是一个长度为n（整数）的字的序列。NGram可以用来将输入特征转换成n-grams。
      val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
      val ngramDataFrame = ngram.transform(wordDataFrame)

      /**
      +---+------------------------------------------+------------------------------------------------------------------+
      |id |words                                     |ngrams                                                            |
      +---+------------------------------------------+------------------------------------------------------------------+
      |0  |[Hi, I, heard, about, Spark]              |[Hi I, I heard, heard about, about Spark]                         |
      |1  |[I, wish, Java, could, use, case, classes]|[I wish, wish Java, Java could, could use, use case, case classes]|
      |2  |[Logistic, regression, models, are, neat] |[Logistic regression, regression models, models are, are neat]    |
      +---+------------------------------------------+------------------------------------------------------------------+
        */
      ngramDataFrame.show(false)
      //ngramDataFrame.select("ngrams").show(false)
    }
}
