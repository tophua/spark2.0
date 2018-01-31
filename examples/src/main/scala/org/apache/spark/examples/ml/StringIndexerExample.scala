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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.feature.StringIndexer
// $example off$

object StringIndexerExample extends SparkCommant{
  def main(args: Array[String]): Unit = {

    // $example on$
    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")
    /**
      * StringIndexer(字符串-索引变换)将字符串的(以单词为)标签编码成标签索引(表示)。
      * 标签索引序列的取值范围是[0，numLabels（字符串中所有出现的单词去掉重复的词后的总和）]，
      * 按照标签出现频率排序，出现最多的标签索引为0。如果输入是数值型，我们先将数值映射到字符串，再对字符串进行索引化
      * 按label出现的频次，转换成0～num numOfLabels-1(分类个数)，频次最高的转换为0，以此类推：
        label=3，出现次数最多，出现了4次，转换（编号）为0
        其次是label=2，出现了3次，编号为1，以此类推
        +-----+------------+
        |label|indexedLabel|
        +-----+------------+
        |3.0  |0.0         |
        |4.0  |3.0         |
        |1.0  |2.0         |
        |3.0  |0.0         |
        |2.0  |1.0         |
        |3.0  |0.0         |
        |2.0  |1.0         |
        |3.0  |0.0         |
        |2.0  |1.0         |
        |1.0  |2.0         |
        +-----+------------+
      在其它地方应用StringIndexer时还需要注意两个问题：
    （1）StringIndexer本质上是对String类型–>index( number);如果是：数值(numeric)–>index(number),
        实际上是对把数值先进行了类型转换(cast numeric to string and then index the string values.),
        也就是说无论是String，还是数值，都可以重新编号（Index);
    （2）利用获得的模型转化新数据集时，可能遇到异常情况，见下面例子。
      */
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)

    /**
      +---+--------+-------------+
      | id|category|categoryIndex|
      +---+--------+-------------+
      |  0|       a|          0.0|
      |  1|       b|          2.0|
      |  2|       c|          1.0|
      |  3|       a|          0.0|
      |  4|       a|          0.0|
      |  5|       c|          1.0|
      +---+--------+-------------+
      */
    indexed.show()
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
