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

package org.apache.spark.sql.execution.columnar.compression

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar.{BOOLEAN, NoopColumnStats}
import org.apache.spark.sql.execution.columnar.ColumnarTestUtils._

class BooleanBitSetSuite extends SparkFunSuite {
  import BooleanBitSet._

  def skeleton(count: Int) {
    // -------------
    // Tests encoder
    // -------------

    val builder = TestCompressibleColumnBuilder(new NoopColumnStats, BOOLEAN, BooleanBitSet)
    val rows = Seq.fill[InternalRow](count)(makeRandomRow(BOOLEAN))
    val values = rows.map(_.getBoolean(0))

    rows.foreach(builder.appendFrom(_, 0))
    val buffer = builder.build()

    // Column type ID + null count + null positions
    //列类型ID +空数量+空位置
    val headerSize = CompressionScheme.columnHeaderSize(buffer)

    // Compression scheme ID + element count + bitset words
    //压缩方案ID +元素计数+位集字
    val compressedSize = 4 + 4 + {
      val extra = if (count % BITS_PER_LONG == 0) 0 else 1
      (count / BITS_PER_LONG + extra) * 8
    }

    // 4 extra bytes for compression scheme type ID
    //4个额外的字节用于压缩方案类型ID
    assertResult(headerSize + compressedSize, "Wrong buffer capacity")(buffer.capacity)

    // Skips column header
    buffer.position(headerSize)
    assertResult(BooleanBitSet.typeId, "Wrong compression scheme ID")(buffer.getInt())
    assertResult(count, "Wrong element count")(buffer.getInt())

    var word = 0: Long
    for (i <- 0 until count) {
      val bit = i % BITS_PER_LONG
      word = if (bit == 0) buffer.getLong() else word
      assertResult(values(i), s"Wrong value in compressed buffer, index=$i") {
        (word & ((1: Long) << bit)) != 0
      }
    }

    // -------------
    // Tests decoder
    // -------------

    // Rewinds, skips column header and 4 more bytes for compression scheme ID
    //倒退，跳过列标题和4个字节的压缩方案ID
    buffer.rewind().position(headerSize + 4)

    val decoder = BooleanBitSet.decoder(buffer, BOOLEAN)
    val mutableRow = new GenericInternalRow(1)
    if (values.nonEmpty) {
      values.foreach {
        assert(decoder.hasNext)
        assertResult(_, "Wrong decoded value") {
          decoder.next(mutableRow, 0)
          mutableRow.getBoolean(0)
        }
      }
    }
    assert(!decoder.hasNext)
  }

  test(s"$BooleanBitSet: empty") {
    skeleton(0)
  }

  test(s"$BooleanBitSet: less than 1 word") {
    skeleton(BITS_PER_LONG - 1)
  }

  test(s"$BooleanBitSet: exactly 1 word") {
    skeleton(BITS_PER_LONG)
  }

  test(s"$BooleanBitSet: multiple whole words") {
    skeleton(BITS_PER_LONG * 2)
  }

  test(s"$BooleanBitSet: multiple words and 1 more bit") {
    skeleton(BITS_PER_LONG * 2 + 1)
  }
}
