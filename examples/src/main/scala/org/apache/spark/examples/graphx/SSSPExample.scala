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
package org.apache.spark.examples.graphx

// $example on$
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example use the Pregel operator to express computation
 * such as single source shortest path
  * 一个例子使用Pregel算子来表达诸如单源最短路径之类的计算
 * Run with
 * {{{
 * bin/run-example graphx.SSSPExample
 * }}}
 */
object SSSPExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // A graph with edge attributes containing distances
    //具有包含距离的边缘属性的图形
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42 // The ultimate source 最终的来源
    // Initialize the graph such that all vertices except the root have distance infinity.
    //初始化图形,使得除根外的所有顶点都具有距离无穷大
    val initialGraph = graph.mapVertices((id, _) =>
        if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program 顶点程序
      triplet => {  // Send Message 发送信息
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message 合并消息
    )
    println(sssp.vertices.collect.mkString("\n"))
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
