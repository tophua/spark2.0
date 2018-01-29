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

import java.util.ConcurrentModificationException

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.memory.MemoryTestingUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class ExternalAppendOnlyUnsafeRowArraySuite extends SparkFunSuite with LocalSparkContext {
  private val random = new java.util.Random()
  private var taskContext: TaskContext = _

  override def afterAll(): Unit = TaskContext.unset()

  private def withExternalArray(inMemoryThreshold: Int, spillThreshold: Int)
                               (f: ExternalAppendOnlyUnsafeRowArray => Unit): Unit = {
    sc = new SparkContext("local", "test", new SparkConf(false))

    taskContext = MemoryTestingUtils.fakeTaskContext(SparkEnv.get)
    TaskContext.setTaskContext(taskContext)

    val array = new ExternalAppendOnlyUnsafeRowArray(
      taskContext.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      taskContext,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      inMemoryThreshold,
      spillThreshold)
    try f(array) finally {
      array.clear()
    }
  }

  private def insertRow(array: ExternalAppendOnlyUnsafeRowArray): Long = {
    val valueInserted = random.nextLong()

    val row = new UnsafeRow(1)
    row.pointTo(new Array[Byte](64), 16)
    row.setLong(0, valueInserted)
    array.add(row)
    valueInserted
  }

  private def checkIfValueExists(iterator: Iterator[UnsafeRow], expectedValue: Long): Unit = {
    assert(iterator.hasNext)
    val actualRow = iterator.next()
    assert(actualRow.getLong(0) == expectedValue)
    assert(actualRow.getSizeInBytes == 16)
  }

  private def validateData(
      array: ExternalAppendOnlyUnsafeRowArray,
      expectedValues: ArrayBuffer[Long]): Iterator[UnsafeRow] = {
    val iterator = array.generateIterator()
    for (value <- expectedValues) {
      checkIfValueExists(iterator, value)
    }

    assert(!iterator.hasNext)
    iterator
  }

  private def populateRows(
      array: ExternalAppendOnlyUnsafeRowArray,
      numRowsToBePopulated: Int): ArrayBuffer[Long] = {
    val populatedValues = new ArrayBuffer[Long]
    populateRows(array, numRowsToBePopulated, populatedValues)
  }

  private def populateRows(
      array: ExternalAppendOnlyUnsafeRowArray,
      numRowsToBePopulated: Int,
      populatedValues: ArrayBuffer[Long]): ArrayBuffer[Long] = {
    for (_ <- 0 until numRowsToBePopulated) {
      populatedValues.append(insertRow(array))
    }
    populatedValues
  }

  private def getNumBytesSpilled: Long = {
    TaskContext.get().taskMetrics().memoryBytesSpilled
  }

  private def assertNoSpill(): Unit = {
    assert(getNumBytesSpilled == 0)
  }

  private def assertSpill(): Unit = {
    assert(getNumBytesSpilled > 0)
  }
  //插入少于inMemoryThreshold的行
  test("insert rows less than the inMemoryThreshold") {
    val (inMemoryThreshold, spillThreshold) = (100, 50)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      assert(array.isEmpty)

      val expectedValues = populateRows(array, 1)
      assert(!array.isEmpty)
      assert(array.length == 1)

      val iterator1 = validateData(array, expectedValues)

      // Add more rows (but not too many to trigger switch to [[UnsafeExternalSorter]])
      // Verify that NO spill has happened
      populateRows(array, inMemoryThreshold - 1, expectedValues)
      assert(array.length == inMemoryThreshold)
      assertNoSpill()

      val iterator2 = validateData(array, expectedValues)

      assert(!iterator1.hasNext)
      assert(!iterator2.hasNext)
    }
  }
  //插入超过inMemoryThreshold但小于spillThreshold的行
  test("insert rows more than the inMemoryThreshold but less than spillThreshold") {
    val (inMemoryThreshold, spillThreshold) = (10, 50)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      assert(array.isEmpty)
      val expectedValues = populateRows(array, inMemoryThreshold - 1)
      assert(array.length == (inMemoryThreshold - 1))
      val iterator1 = validateData(array, expectedValues)
      assertNoSpill()

      // Add more rows to trigger switch to [[UnsafeExternalSorter]] but not too many to cause a
      // spill to happen. Verify that NO spill has happened
      //添加更多的行触发切换到[[UnsafeExternalSorter]],但不是太多,导致溢出发生,确认没有溢出发生
      populateRows(array, spillThreshold - expectedValues.length - 1, expectedValues)
      assert(array.length == spillThreshold - 1)
      assertNoSpill()

      val iterator2 = validateData(array, expectedValues)
      assert(!iterator2.hasNext)

      assert(!iterator1.hasNext)
      intercept[ConcurrentModificationException](iterator1.next())
    }
  }
  //插入行足以强制泄漏
  test("insert rows enough to force spill") {
    val (inMemoryThreshold, spillThreshold) = (20, 10)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      assert(array.isEmpty)
      val expectedValues = populateRows(array, inMemoryThreshold - 1)
      assert(array.length == (inMemoryThreshold - 1))
      val iterator1 = validateData(array, expectedValues)
      assertNoSpill()

      // Add more rows to trigger switch to [[UnsafeExternalSorter]] and cause a spill to happen.
      // Verify that spill has happened
      populateRows(array, 2, expectedValues)
      assert(array.length == inMemoryThreshold + 1)
      assertSpill()

      val iterator2 = validateData(array, expectedValues)
      assert(!iterator2.hasNext)

      assert(!iterator1.hasNext)
      intercept[ConcurrentModificationException](iterator1.next())
    }
  }
  //空数组上的迭代器应该是空的
  test("iterator on an empty array should be empty") {
    withExternalArray(inMemoryThreshold = 4, spillThreshold = 10) { array =>
      val iterator = array.generateIterator()
      assert(array.isEmpty)
      assert(array.length == 0)
      assert(!iterator.hasNext)
    }
  }
  //生成具有负启动索引的迭代器
  test("generate iterator with negative start index") {
    withExternalArray(inMemoryThreshold = 100, spillThreshold = 56) { array =>
      val exception =
        intercept[ArrayIndexOutOfBoundsException](array.generateIterator(startIndex = -10))

      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array")
      )
    }
  }
  //生成具有超出数组大小的开始索引的迭代器（无溢出）
  test("generate iterator with start index exceeding array's size (without spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      populateRows(array, spillThreshold / 2)

      val exception =
        intercept[ArrayIndexOutOfBoundsException](
          array.generateIterator(startIndex = spillThreshold * 10))
      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array"))
    }
  }
  //生成具有超出数组大小的开始索引的迭代器（溢出）
  test("generate iterator with start index exceeding array's size (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      populateRows(array, spillThreshold * 2)

      val exception =
        intercept[ArrayIndexOutOfBoundsException](
          array.generateIterator(startIndex = spillThreshold * 10))

      assert(exception.getMessage.contains(
        "Invalid `startIndex` provided for generating iterator over the array"))
    }
  }
  //使用自定义开始索引生成迭代器（没有溢出）
  test("generate iterator with custom start index (without spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      val expectedValues = populateRows(array, inMemoryThreshold)
      val startIndex = inMemoryThreshold / 2
      val iterator = array.generateIterator(startIndex = startIndex)
      for (i <- startIndex until expectedValues.length) {
        checkIfValueExists(iterator, expectedValues(i))
      }
    }
  }
  //用自定义开始索引生成迭代器（有溢出）
  test("generate iterator with custom start index (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (20, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      val expectedValues = populateRows(array, spillThreshold * 10)
      val startIndex = spillThreshold * 2
      val iterator = array.generateIterator(startIndex = startIndex)
      for (i <- startIndex until expectedValues.length) {
        checkIfValueExists(iterator, expectedValues(i))
      }
    }
  }
  //测试迭代器失效（无溢出）
  test("test iterator invalidation (without spill)") {
    withExternalArray(inMemoryThreshold = 10, spillThreshold = 100) { array =>
      // insert 2 rows, iterate until the first row
      //插入2行，迭代直到第一行
      populateRows(array, 2)

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      //添加更多的行应该使旧的迭代器失效
      populateRows(array, 1)
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())

      // Clearing the array should also invalidate any old iterators
      //清除数组也应使任何旧的迭代器无效
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }
  //测试迭代器失效（溢出）
  test("test iterator invalidation (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (2, 10)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      // Populate enough rows so that spill happens
      //填充足够的行，以便发生溢出
      populateRows(array, spillThreshold * 2)
      assertSpill()

      var iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      // Adding more row(s) should invalidate any old iterators
      //添加更多的行应该使旧的迭代器失效
      populateRows(array, 1)
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())

      // Clearing the array should also invalidate any old iterators
      //清除数组也应使任何旧的迭代器无效
      iterator = array.generateIterator()
      assert(iterator.hasNext)
      iterator.next()

      array.clear()
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }
  //清空数组
  test("clear on an empty the array") {
    withExternalArray(inMemoryThreshold = 2, spillThreshold = 3) { array =>
      val iterator = array.generateIterator()
      assert(!iterator.hasNext)

      // multiple clear'ing should not have an side-effect
      //多重清除不应有副作用
      array.clear()
      array.clear()
      array.clear()
      assert(array.isEmpty)
      assert(array.length == 0)

      // Clearing an empty array should also invalidate any old iterators
      //清空一个空数组也应该使任何旧的迭代器无效
      assert(!iterator.hasNext)
      intercept[ConcurrentModificationException](iterator.next())
    }
  }
  //清除数组（无溢出）
  test("clear array (without spill)") {
    val (inMemoryThreshold, spillThreshold) = (10, 100)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      // Populate rows ... but not enough to trigger spill
      //填充行...但不足以触发溢出
      populateRows(array, inMemoryThreshold / 2)
      assertNoSpill()

      // Clear the array
      // 清除数组
      array.clear()
      assert(array.isEmpty)

      // Re-populate few rows so that there is no spill
      //重新填充几行，以防止泄漏
      // Verify the data. Verify that there was no spill
      //验证数据,确认没有泄漏
      val expectedValues = populateRows(array, inMemoryThreshold / 2)
      validateData(array, expectedValues)
      assertNoSpill()

      // Populate more rows .. enough to not trigger a spill.
      //填充更多的行..足以不引发溢出。
      // Verify the data. Verify that there was no spill
      //验证数据。 确认没有泄漏
      populateRows(array, inMemoryThreshold / 2, expectedValues)
      validateData(array, expectedValues)
      assertNoSpill()
    }
  }
  //清除数组（溢出）
  test("clear array (with spill)") {
    val (inMemoryThreshold, spillThreshold) = (10, 20)
    withExternalArray(inMemoryThreshold, spillThreshold) { array =>
      // Populate enough rows to trigger spill
      //填充足够的行触发溢出
      populateRows(array, spillThreshold * 2)
      val bytesSpilled = getNumBytesSpilled
      assert(bytesSpilled > 0)

      // Clear the array 清除数组
      array.clear()
      assert(array.isEmpty)

      // Re-populate the array ... but NOT upto the point that there is spill.
      //重新填充数组...但不是溢出的点。
      // Verify data. Verify that there was NO "extra" spill
      //验证数据。 确认没有“额外”泄漏
      val expectedValues = populateRows(array, spillThreshold / 2)
      validateData(array, expectedValues)
      assert(getNumBytesSpilled == bytesSpilled)

      // Populate more rows to trigger spill
      //填充更多的行来触发溢出
      // Verify the data. Verify that there was "extra" spill
      //验证数据。 确认有“额外”泄漏
      populateRows(array, spillThreshold * 2, expectedValues)
      validateData(array, expectedValues)
      assert(getNumBytesSpilled > bytesSpilled)
    }
  }
}
