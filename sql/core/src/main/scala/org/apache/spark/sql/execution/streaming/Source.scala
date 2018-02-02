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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * A source of continually arriving data for a streaming query. A [[Source]] must have a
 * monotonically increasing notion of progress that can be represented as an [[Offset]]. Spark
 * will regularly query each [[Source]] to see if any more data is available.
  * 持续到达的流式查询数据的来源,[[Source]]必须具有单调递增的进展概念，可以表示为[偏移量 Offset],
  * Spark会定期查询每个[[Source]]是否有更多数据可用。
 */
trait Source {

  /** Returns the schema of the data from this source
    * 返回此源数据的模式*/
  def schema: StructType

  /**
   * Returns the maximum available offset for this source.
    * 返回此源的最大可用偏移量
   * Returns `None` if this source has never received any data.
    * 如果此源从未收到任何数据,则返回“无”
   */
  def getOffset: Option[Offset]

  /**
   * Returns the data that is between the offsets (`start`, `end`]. When `start` is `None`,
   * then the batch should begin with the first record. This method must always return the
   * same data for a particular `start` and `end` pair; even after the Source has been restarted
   * on a different node.
    *
    * 返回偏移量之间的数据（`start`，`end`]。当'start'是'None'时,批处理应该从第一条记录开始,
    * 这个方法必须始终返回相同的数据 `和`end`对;即使源已经在不同的节点上重新启动
   *
   * Higher layers will always call this method with a value of `start` greater than or equal
   * to the last value passed to `commit` and a value of `end` less than or equal to the
   * last value returned by `getOffset`
    * 更高的层将总是调用此方法,其值为“start”大于或等于传递给commit的最后一个值,“end”的值小于或等于`getOffset`返回的最后一个值
   *
   * It is possible for the [[Offset]] type to be a [[SerializedOffset]] when it was
   * obtained from the log. Moreover, [[StreamExecution]] only compares the [[Offset]]
   * JSON representation to determine if the two objects are equal. This could have
   * ramifications when upgrading [[Offset]] JSON formats i.e., two equivalent [[Offset]]
   * objects could differ between version. Consequently, [[StreamExecution]] may call
   * this method with two such equivalent [[Offset]] objects. In which case, the [[Source]]
   * should return an empty [[DataFrame]]
    *
    * 从[日志]中获得[[Offset]]类型可能是[[SerializedOffset]], 此外,[[StreamExecution]]仅比较[[Offset]]
    * JSON表示以确定两个对象是否相等,当升级[[Offset]] JSON格式时,这可能会产生影响,
    * 即两个等价的[[Offset]]对象在版本之间可能有所不同,因此,[[StreamExecution]]
    * 可以用两个这样的等价的[[Offset]]对象来调用这个方法,在这种情况下,[[Source]]应该返回一个空的[[DataFrame]]
   */
  def getBatch(start: Option[Offset], end: Offset): DataFrame

  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
    * 通知Spark已完成处理小于或等于'end'的所有偏移量的数据,并且将来只会请求大于'end'的偏移量
   */
  def commit(end: Offset) : Unit = {}

  /** Stop this source and free any resources it has allocated.
    * 停止此源并释放已分配的任何资源*/
  def stop(): Unit
}
