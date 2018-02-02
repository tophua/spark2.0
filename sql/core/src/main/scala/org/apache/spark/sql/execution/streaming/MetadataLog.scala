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

/**
 * A general MetadataLog that supports the following features:
 * 支持以下功能的通用MetadataLog：
 *  - Allow the user to store a metadata object for each batch.
  *  允许用户为每个批次存储元数据对象
 *  - Allow the user to query the latest batch id.允许用户查询最新的批次ID
 *  - Allow the user to query the metadata object of a specified batch id.
  *  允许用户查询指定批次ID的元数据对象
 *  - Allow the user to query metadata objects in a range of batch ids.
  *  允许用户查询一批批ID中的元数据对象
 *  - Allow the user to remove obsolete metadata
  *  允许用户删除过时的元数据
 */
trait MetadataLog[T] {

  /**
   * Store the metadata for the specified batchId and return `true` if successful. If the batchId's
   * metadata has already been stored, this method will return `false`.
    * 存储指定的batchId的元数据,如果成功则返回“true”,如果batchId的元数据已经被存储了,这个方法将会返回`false`。
   */
  def add(batchId: Long, metadata: T): Boolean

  /**
   * Return the metadata for the specified batchId if it's stored. Otherwise, return None.
    * 返回指定的batchId的元数据(如果存储的话),否则,返回None
   */
  def get(batchId: Long): Option[T]

  /**
   * Return metadata for batches between startId (inclusive) and endId (inclusive). If `startId` is
   * `None`, just return all batches before endId (inclusive).
    * 返回startId(含)和endId(含)之间批量的元数据,如果`startId`是`None`,只需返回endId(含)之前的所有批。
   */
  def get(startId: Option[Long], endId: Option[Long]): Array[(Long, T)]

  /**
   * Return the latest batch Id and its metadata if exist.
    * 返回最新的批次标识及其元数据(如果存在)
   */
  def getLatest(): Option[(Long, T)]

  /**
   * Removes all the log entry earlier than thresholdBatchId (exclusive).
    * 删除thresholdBatchId（独占）之前的所有日志条目
   * This operation should be idempotent.
    * 这个操作应该是幂等的
   */
  def purge(thresholdBatchId: Long): Unit
}
