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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.internal.SQLConf

/** A class that contains configuration parameters for [[StateStore]]s.
  * 包含[[StateStore]]的配置参数的类 */
class StateStoreConf(@transient private val sqlConf: SQLConf)
  extends Serializable {

  def this() = this(new SQLConf)

  /**
   * Minimum number of delta files in a chain after which HDFSBackedStateStore will
   * consider generating a snapshot.
    * HDFSBackedStateStore将考虑生成快照之后,链中的最小数量的增量文件
   */
  val minDeltasForSnapshot: Int = sqlConf.stateStoreMinDeltasForSnapshot

  /** Minimum versions a State Store implementation should retain to allow rollbacks
    * State Store实现应保留的最小版本允许回滚*/
  val minVersionsToRetain: Int = sqlConf.minBatchesToRetain

  /**
   * Optional fully qualified name of the subclass of [[StateStoreProvider]]
   * managing state data. That is, the implementation of the State Store to use.
    * [[StateStoreProvider]]管理状态数据的子类的可选完全限定名称,也就是说,存储的实施使用
   */
  val providerClass: String = sqlConf.stateStoreProviderClass

  /**
   * Additional configurations related to state store. This will capture all configs in
   * SQLConf that start with `spark.sql.streaming.stateStore.`
    * 与状态存储相关的其他配置,这将捕获以`spark.sql.streaming.stateStore.`开头的SQLConf中的所有配置*/
  val confs: Map[String, String] =
    sqlConf.getAllConfs.filter(_._1.startsWith("spark.sql.streaming.stateStore."))
}

object StateStoreConf {
  val empty = new StateStoreConf()

  def apply(conf: SQLConf): StateStoreConf = new StateStoreConf(conf)
}
