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

package org.apache.spark.sql.streaming

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset}

trait OffsetSuite extends SparkFunSuite {
  /** Creates test to check all the comparisons of offsets given a `one` that is less than `two`.
    * 创建测试来检查给定“one”小于“two”的偏移量的所有比较*/
  def compare(one: Offset, two: Offset): Unit = {
    test(s"comparison $one <=> $two") {
      assert(one == one)
      assert(two == two)
      assert(one != two)
      assert(two != one)
    }
  }
}
//长偏移距套件
class LongOffsetSuite extends OffsetSuite {
  val one = LongOffset(1)
  val two = LongOffset(2)
  val three = LongOffset(3)
  compare(one, two)

  compare(LongOffset(SerializedOffset(one.json)),
          LongOffset(SerializedOffset(three.json)))
}


