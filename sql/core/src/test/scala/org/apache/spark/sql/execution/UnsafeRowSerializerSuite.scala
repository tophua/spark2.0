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

package org.apache.spark.sql.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/**
 * used to test close InputStream in UnsafeRowSerializer
  * 用于在UnsafeRowSerializer中测试关闭InputStream
 */
class ClosableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {
  var closed: Boolean = false
  override def close(): Unit = {
    closed = true
    super.close()
  }
}

class UnsafeRowSerializerSuite extends SparkFunSuite with LocalSparkContext {

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

  test("toUnsafeRow() test helper method") {
    // This currently doesnt work because the generic getter throws an exception.
    //这目前不工作,因为泛型getter引发异常。
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    assert(row.getString(0) === unsafeRow.getUTF8String(0).toString)
    assert(row.getInt(1) === unsafeRow.getInt(1))
  }
  //基本的行序列化

  test("basic row serialization") {
    val rows = Seq(Row("Hello", 1), Row("World", 2))
    val unsafeRows = rows.map(row => toUnsafeRow(row, Array(StringType, IntegerType)))
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val baos = new ByteArrayOutputStream()
    val serializerStream = serializer.serializeStream(baos)
    for (unsafeRow <- unsafeRows) {
      serializerStream.writeKey(0)
      serializerStream.writeValue(unsafeRow)
    }
    serializerStream.close()
    val input = new ClosableByteArrayInputStream(baos.toByteArray)
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    for (expectedRow <- unsafeRows) {
      val actualRow = deserializerIter.next().asInstanceOf[(Integer, UnsafeRow)]._2
      assert(expectedRow.getSizeInBytes === actualRow.getSizeInBytes)
      assert(expectedRow.getString(0) === actualRow.getString(0))
      assert(expectedRow.getInt(1) === actualRow.getInt(1))
    }
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }
  //关闭空输入流
  test("close empty input stream") {
    val input = new ClosableByteArrayInputStream(Array.empty)
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }
  //外部分拣机溢出与不安全的行序列化器“
  test("SPARK-10466: external sorter spilling with unsafe row serializer") {
    var sc: SparkContext = null
    var outputFile: File = null
    //保存旧的SparkEnv，因为它将被覆盖
    val oldEnv = SparkEnv.get // save the old SparkEnv, as it will be overwritten
    Utils.tryWithSafeFinally {
      val conf = new SparkConf()
        .set("spark.shuffle.spill.initialMemoryThreshold", "1")
        .set("spark.shuffle.sort.bypassMergeThreshold", "0")
        .set("spark.testing.memory", "80000")

      sc = new SparkContext("local", "test", conf)
      outputFile = File.createTempFile("test-unsafe-row-serializer-spill", "")
      // prepare data
      val converter = unsafeRowConverter(Array(IntegerType))
      val data = (1 to 10000).iterator.map { i =>
        (i, converter(Row(i)))
      }
      val taskMemoryManager = new TaskMemoryManager(sc.env.memoryManager, 0)
      val taskContext = new TaskContextImpl(0, 0, 0, 0, taskMemoryManager, new Properties, null)

      val sorter = new ExternalSorter[Int, UnsafeRow, UnsafeRow](
        taskContext,
        partitioner = Some(new HashPartitioner(10)),
        serializer = new UnsafeRowSerializer(numFields = 1))

      // Ensure we spilled something and have to merge them later
      //确保我们泄漏了一些东西，以后再合并
      assert(sorter.numSpills === 0)
      sorter.insertAll(data)
      assert(sorter.numSpills > 0)

      // Merging spilled files should not throw assertion error
      //合并溢出的文件不应该抛出断言错误
      sorter.writePartitionedFile(ShuffleBlockId(0, 0, 0), outputFile)
    } {
      // Clean up
      if (sc != null) {
        sc.stop()
      }

      // restore the spark env
      //恢复Spark环境
      SparkEnv.set(oldEnv)

      if (outputFile != null) {
        outputFile.delete()
      }
    }
  }
  //不安全的行序列化器与SortShuffleManager
  test("SPARK-10403: unsafe row serializer with SortShuffleManager") {
    val conf = new SparkConf().set("spark.shuffle.manager", "sort")
    sc = new SparkContext("local", "test", conf)
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    val rowsRDD = sc.parallelize(Seq((0, unsafeRow), (1, unsafeRow), (0, unsafeRow)))
      .asInstanceOf[RDD[Product2[Int, InternalRow]]]
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rowsRDD,
        new PartitionIdPassthrough(2),
        new UnsafeRowSerializer(2))
    val shuffled = new ShuffledRowRDD(dependency)
    shuffled.count()
  }
}
