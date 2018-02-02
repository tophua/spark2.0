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

/**
 * An interface for systems that can collect the results of a streaming query. In order to preserve
 * exactly once semantics a sink must be idempotent in the face of multiple attempts to add the same
 * batch.
  * 一个可以收集流式查询结果的系统界面,为了保留一次语义,在多次尝试添加相同批次的情况下,汇必须是幂等的
 */
trait Sink {

  /**
   * Adds a batch of data to this sink. The data for a given `batchId` is deterministic and if
   * this method is called more than once with the same batchId (which will happen in the case of
   * failures), then `data` should only be added once.
    * 将一批数据添加到此接收器,给定的`batchId`的数据是确定性的,如果这个方法被多次调用同一个batchId（这将在失败的情况下发生）
    * ,那么`data`应该只添加一次。
   *
   * Note 1: You cannot apply any operators on `data` except consuming it (e.g., `collect/foreach`).
   * Otherwise, you may get a wrong result.
   * 注1：除了消费之外，你不能在`data`上应用任何操作符（例如`collect / foreach`）。 否则，你可能会得到一个错误的结果。
   * Note 2: The method is supposed to be executed synchronously, i.e. the method should only return
   * after data is consumed by sink successfully.
    * 注2：该方法应该是同步执行,即该方法只应在数据被接收器成功消耗后才能返回
   */
  def addBatch(batchId: Long, data: DataFrame): Unit
}
