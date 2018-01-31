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
import org.apache.spark.ml.feature.{Interaction, VectorAssembler}
// $example off$

object InteractionExample extends SparkCommant{
  def main(args: Array[String]): Unit = {


    // $example on$
    val df = spark.createDataFrame(Seq(
      (1, 1, 2, 3, 8, 4, 5),
      (2, 4, 3, 8, 7, 9, 8),
      (3, 6, 1, 9, 2, 3, 6),
      (4, 10, 8, 6, 9, 4, 5),
      (5, 9, 2, 7, 10, 7, 3),
      (6, 1, 1, 4, 2, 8, 4)
    )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")
    //VectorAssembler是一个transformer,将多列数据转化为单列的向量列
    //为方便后续模型进行特征输入,需要部分列的数据转换为特征向量,并统一命名
    val assembler1 = new VectorAssembler().
      setInputCols(Array("id2", "id3", "id4")).
      setOutputCol("vec1")

    val assembled1 = assembler1.transform(df)

    /**
      +---+---+---+---+---+---+---+--------------+
      |id1|id2|id3|id4|id5|id6|id7|          vec1|
      +---+---+---+---+---+---+---+--------------+
      |  1|  1|  2|  3|  8|  4|  5| [1.0,2.0,3.0]|
      |  2|  4|  3|  8|  7|  9|  8| [4.0,3.0,8.0]|
      |  3|  6|  1|  9|  2|  3|  6| [6.0,1.0,9.0]|
      |  4| 10|  8|  6|  9|  4|  5|[10.0,8.0,6.0]|
      |  5|  9|  2|  7| 10|  7|  3| [9.0,2.0,7.0]|
      |  6|  1|  1|  4|  2|  8|  4| [1.0,1.0,4.0]|
      +---+---+---+---+---+---+---+--------------+
      */
    assembled1.show()
    val assembler2 = new VectorAssembler().
      setInputCols(Array("id5", "id6", "id7")).
      setOutputCol("vec2")

    /**
      +---+---+---+---+---+---+---+--------------+
      |id1|id2|id3|id4|id5|id6|id7|          vec1|
      +---+---+---+---+---+---+---+--------------+
      |  1|  1|  2|  3|  8|  4|  5| [1.0,2.0,3.0]|
      |  2|  4|  3|  8|  7|  9|  8| [4.0,3.0,8.0]|
      |  3|  6|  1|  9|  2|  3|  6| [6.0,1.0,9.0]|
      |  4| 10|  8|  6|  9|  4|  5|[10.0,8.0,6.0]|
      |  5|  9|  2|  7| 10|  7|  3| [9.0,2.0,7.0]|
      |  6|  1|  1|  4|  2|  8|  4| [1.0,1.0,4.0]|
      +---+---+---+---+---+---+---+--------------+
      */
    assembled1.show()
    val assembled2 = assembler2.transform(assembled1).select("id1", "vec1", "vec2")
    //将多列数据转化为单列的向量列,其中每个列的一个值的组合乘积
    val interaction = new Interaction()
      .setInputCols(Array("id1", "vec1", "vec2"))
      .setOutputCol("interactedCol")

    val interacted = interaction.transform(assembled2)

    /**
      +---+--------------+--------------+------------------------------------------------------+
      |id1|vec1          |vec2          |interactedCol                                         |
      +---+--------------+--------------+------------------------------------------------------+
      |1  |[1.0,2.0,3.0] |[8.0,4.0,5.0] |[8.0,4.0,5.0,16.0,8.0,10.0,24.0,12.0,15.0]            |
      |2  |[4.0,3.0,8.0] |[7.0,9.0,8.0] |[56.0,72.0,64.0,42.0,54.0,48.0,112.0,144.0,128.0]     |
      |3  |[6.0,1.0,9.0] |[2.0,3.0,6.0] |[36.0,54.0,108.0,6.0,9.0,18.0,54.0,81.0,162.0]        |
      |4  |[10.0,8.0,6.0]|[9.0,4.0,5.0] |[360.0,160.0,200.0,288.0,128.0,160.0,216.0,96.0,120.0]|
      |5  |[9.0,2.0,7.0] |[10.0,7.0,3.0]|[450.0,315.0,135.0,100.0,70.0,30.0,350.0,245.0,105.0] |
      |6  |[1.0,1.0,4.0] |[2.0,8.0,4.0] |[12.0,48.0,24.0,12.0,48.0,24.0,48.0,192.0,96.0]       |
      +---+--------------+--------------+------------------------------------------------------+
      */
    interacted.show(truncate = false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
