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

import java.util.UUID
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Base trait for a versioned key-value store. Each instance of a `StateStore` represents a specific
 * version of state data, and such instances are created through a [[StateStoreProvider]].
  * 版本化键值存储的基本特征,“StateStore”的每个实例代表一个特定版本的状态数据,
  * 而这样的实例是通过[[StateStoreProvider]]创建
 */
trait StateStore {

  /** Unique identifier of the store
    * 存储的唯一标识符*/
  def id: StateStoreId

  /** Version of the data in this store before committing updates.
    * 在提交更新之前,该存储中的数据的版本*/
  def version: Long

  /**
   * Get the current value of a non-null key.
    * 获取非空key的当前值。
   * @return a non-null row if the key exists in the store, otherwise null.
    *         如果存储键中存在非空行,否则为空
   */
  def get(key: UnsafeRow): UnsafeRow

  /**
   * Put a new value for a non-null key. Implementations must be aware that the UnsafeRows in
   * the params can be reused, and must make copies of the data as needed for persistence.
    * 为非null键提供一个新的值,实现必须意识到,参数中的UnsafeRows可以被重用,并且必须根据持久化的需要复制数据。
   */
  def put(key: UnsafeRow, value: UnsafeRow): Unit

  /**
   * Remove a single non-null key.
    * 删除一个非空的key
   */
  def remove(key: UnsafeRow): Unit

  /**
   * Get key value pairs with optional approximate `start` and `end` extents.
    * 获取具有可选近似“开始”和“结束”范围的键值对
   * If the State Store implementation maintains indices for the data based on the optional
   * `keyIndexOrdinal` over fields `keySchema` (see `StateStoreProvider.init()`), then it can use
   * `start` and `end` to make a best-effort scan over the data. Default implementation returns
   * the full data scan iterator, which is correct but inefficient. Custom implementations must
   * ensure that updates (puts, removes) can be made while iterating over this iterator.
   * 如果状态存储实现维护基于字段`keySchema`上的可选`keyIndexOrdinal`的数据索引（参见`StateStoreProvider.init（）`）,
    * 则可以使用`start`和`end`来尽力而为扫描数据。默认实现返回完整的数据扫描迭代器,这是正确的,但效率低下,
    * 自定义实现必须确保在遍历此迭代器时可以进行更新（放入，移除）。
   * @param start UnsafeRow having the `keyIndexOrdinal` column set with appropriate starting value.
   * @param end UnsafeRow having the `keyIndexOrdinal` column set with appropriate ending value.
   * @return An iterator of key-value pairs that is guaranteed not miss any key between start and
   *         end, both inclusive.
   */
  def getRange(start: Option[UnsafeRow], end: Option[UnsafeRow]): Iterator[UnsafeRowPair] = {
    iterator()
  }

  /**
   * Commit all the updates that have been made to the store, and return the new version.
    * 提交已经对存储进行的所有更新,并返回新版本。
   * Implementations should ensure that no more updates (puts, removes) can be after a commit in
   * order to avoid incorrect usage.
    * 实现应该确保提交之后不能再有更新(放入,删除),以避免错误的使用
   */
  def commit(): Long

  /**
   * Abort all the updates that have been made to the store. Implementations should ensure that
   * no more updates (puts, removes) can be after an abort in order to avoid incorrect usage.
    * 中止对存储所做的所有更新,实现应该确保在中止之后不再有更新(放置,删除),以避免不正确的使用
   */
  def abort(): Unit

  /**
   * Return an iterator containing all the key-value pairs in the SateStore. Implementations must
   * ensure that updates (puts, removes) can be made while iterating over this iterator.
    * 返回包含SateStore中所有键值对的迭代器,实现必须确保更新(放置,删除)可以迭代这个迭代器
   */
  def iterator(): Iterator[UnsafeRowPair]

  /** Current metrics of the state store
    * 当前的状态存储指标 */
  def metrics: StateStoreMetrics

  /**
   * Whether all updates have been committed
    * 是否所有更新都已经提交
   */
  def hasCommitted: Boolean
}

/**
 * Metrics reported by a state store
  * 存储报告的指标
 * @param numKeys         Number of keys in the state store 状态存储中的键数
 * @param memoryUsedBytes Memory used by the state store 内存使用的状态存储
 * @param customMetrics   Custom implementation-specific metrics 自定义实施特定指标
 *                        The metrics reported through this must have the same `name` as those
 *                        reported by `StateStoreProvider.customMetrics`.
 */
case class StateStoreMetrics(
    numKeys: Long,
    memoryUsedBytes: Long,
    customMetrics: Map[StateStoreCustomMetric, Long])

/**
 * Name and description of custom implementation-specific metrics that a
 * state store may wish to expose.
  * 存储可能希望公开的自定义实施特定指标的名称和描述
 */
trait StateStoreCustomMetric {
  def name: String
  def desc: String
}
case class StateStoreCustomSizeMetric(name: String, desc: String) extends StateStoreCustomMetric
case class StateStoreCustomTimingMetric(name: String, desc: String) extends StateStoreCustomMetric

/**
 * Trait representing a provider that provide [[StateStore]] instances representing
 * versions of state data.
 *
 * The life cycle of a provider and its provide stores are as follows.
 *
 * - A StateStoreProvider is created in a executor for each unique [[StateStoreId]] when
 *   the first batch of a streaming query is executed on the executor. All subsequent batches reuse
 *   this provider instance until the query is stopped.
 *
 * - Every batch of streaming data request a specific version of the state data by invoking
 *   `getStore(version)` which returns an instance of [[StateStore]] through which the required
 *   version of the data can be accessed. It is the responsible of the provider to populate
 *   this store with context information like the schema of keys and values, etc.
 *
 * - After the streaming query is stopped, the created provider instances are lazily disposed off.
 */
trait StateStoreProvider {

  /**
   * Initialize the provide with more contextual information from the SQL operator.
    * 使用SQL运算符初始化提供更多上下文信息
   * This method will be called first after creating an instance of the StateStoreProvider by
   * reflection.
   * 在通过反射创建StateStoreProvider的实例之后,将首先调用此方法
   * @param stateStoreId Id of the versioned StateStores that this provider will generate
   * @param keySchema Schema of keys to be stored
   * @param valueSchema Schema of value to be stored
   * @param keyIndexOrdinal Optional column (represent as the ordinal of the field in keySchema) by
   *                        which the StateStore implementation could index the data.
   * @param storeConfs Configurations used by the StateStores
   * @param hadoopConf Hadoop configuration that could be used by StateStore to save state data
   */
  def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyIndexOrdinal: Option[Int], // for sorting the data by their keys
      storeConfs: StateStoreConf,
      hadoopConf: Configuration): Unit

  /**
   * Return the id of the StateStores this provider will generate.
    * 返回此提供程序将生成的StateStore的ID
   * Should be the same as the one passed in init().
    * 应该和init()中传递的一样
   */
  def stateStoreId: StateStoreId

  /** Called when the provider instance is unloaded from the executor
    * 从执行程序卸载提供程序实例时调用*/
  def close(): Unit

  /** Return an instance of [[StateStore]] representing state data of the given version
    * 返回表示给定版本的状态数据的[[StateStore]]的实例*/
  def getStore(version: Long): StateStore

  /** Optional method for providers to allow for background maintenance (e.g. compactions)
    * 提供者的可选方法允许后台维护（例如压缩）*/
  def doMaintenance(): Unit = { }

  /**
   * Optional custom metrics that the implementation may want to report.
    * 实现可能想要报告的可选自定义度量标准
   * @note The StateStore objects created by this provider must report the same custom metrics
   * (specifically, same names) through `StateStore.metrics`.
   */
  def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil
}

object StateStoreProvider {

  /**
   * Return a instance of the given provider class name. The instance will not be initialized.
    * 返回给定提供者类名称的一个实例,实例不会被初始化
   */
  def create(providerClassName: String): StateStoreProvider = {
    val providerClass = Utils.classForName(providerClassName)
    providerClass.newInstance().asInstanceOf[StateStoreProvider]
  }

  /**
   * Return a instance of the required provider, initialized with the given configurations.
    * 返回所需提供程序的实例,使用给定的配置进行初始化
   */
  def createAndInit(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int], // for sorting the data
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStoreProvider = {
    val provider = create(storeConf.providerClass)
    provider.init(stateStoreId, keySchema, valueSchema, indexOrdinal, storeConf, hadoopConf)
    provider
  }
}

/**
 * Unique identifier for a provider, used to identify when providers can be reused.
  * 提供者的唯一标识符,用于标识提供者何时可以重用
 * Note that `queryRunId` is used uniquely identify a provider, so that the same provider
 * instance is not reused across query restarts.
  * 请注意,`queryRunId`用于唯一标识一个提供程序,因此同一提供程序实例在查询重新启动时不会被重用
 */
case class StateStoreProviderId(storeId: StateStoreId, queryRunId: UUID)

/**
 * Unique identifier for a bunch of keyed state data.
  * 一组键控状态数据的唯一标识符
 * @param checkpointRootLocation Root directory where all the state data of a query is stored
  *                               存储查询的所有状态数据的根目录
 * @param operatorId Unique id of a stateful operator 有状态操作符的唯一标识
 * @param partitionId Index of the partition of an operators state data 运算符状态数据分区索引
 * @param storeName Optional, name of the store. Each partition can optionally use multiple state
 *                  stores, but they have to be identified by distinct names.
  *                  可选的,存储的名称,每个分区都可以选择使用多个状态存储,但它们必须由不同的名称进行标识,
 */
case class StateStoreId(
    checkpointRootLocation: String,
    operatorId: Long,
    partitionId: Int,
    storeName: String = StateStoreId.DEFAULT_STORE_NAME) {

  /**
   * Checkpoint directory to be used by a single state store, identified uniquely by the tuple
   * (operatorId, partitionId, storeName). All implementations of [[StateStoreProvider]] should
   * use this path for saving state data, as this ensures that distinct stores will write to
   * different locations.
    * 检查目录是由一个存储使用,唯一的元组标识（operatorId,partitionid，storename）,
    * 所有实现statestoreprovider,应该使用这个路径保存的状态数据,以保证不同的存储将写入到不同的位置
   */
  def storeCheckpointLocation(): Path = {
    if (storeName == StateStoreId.DEFAULT_STORE_NAME) {
      // For reading state store data that was generated before store names were used (Spark <= 2.2)
      //用于读取存储名称之前生成的状态存储数据（Spark< = 2.2）
      new Path(checkpointRootLocation, s"$operatorId/$partitionId")
    } else {
      new Path(checkpointRootLocation, s"$operatorId/$partitionId/$storeName")
    }
  }
}

object StateStoreId {
  val DEFAULT_STORE_NAME = "default"
}

/** Mutable, and reusable class for representing a pair of UnsafeRows.
  * 易变的和可重用的类代表一对unsaferows*/
class UnsafeRowPair(var key: UnsafeRow = null, var value: UnsafeRow = null) {
  def withRows(key: UnsafeRow, value: UnsafeRow): UnsafeRowPair = {
    this.key = key
    this.value = value
    this
  }
}


/**
 * Companion object to [[StateStore]] that provides helper methods to create and retrieve stores
 * by their unique ids. In addition, when a SparkContext is active (i.e. SparkEnv.get is not null),
  * 同伴对象statestore提供的辅助方法来创建和检索存储的ID。此外,当一个sparkcontext是活跃的(即sparkenv.get不为空)
 * it also runs a periodic background task to do maintenance on the loaded stores. For each
 * store, it uses the [[StateStoreCoordinator]] to ensure whether the current loaded instance of
 * the store is the active instance. Accordingly, it either keeps it loaded and performs
 * maintenance, or unloads the store.
  *
  * 它还运行一个定期后台任务来对加载的存储进行维护,每一个存储它使用statestorecoordinator是否当前加载的实例的存储活动实例。
  * 因此,它可以保持它的加载和执行维护,或存储
 */
object StateStore extends Logging {

  val MAINTENANCE_INTERVAL_CONFIG = "spark.sql.streaming.stateStore.maintenanceInterval"
  val MAINTENANCE_INTERVAL_DEFAULT_SECS = 60

  @GuardedBy("loadedProviders")
  private val loadedProviders = new mutable.HashMap[StateStoreProviderId, StateStoreProvider]()

  /**
   * Runs the `task` periodically and automatically cancels it if there is an exception. `onError`
   * will be called when an exception happens.
    * 周期性地运行“任务”,如果有异常,则自动取消它,OnError将被称为当发生异常时
   */
  class MaintenanceTask(periodMs: Long, task: => Unit, onError: => Unit) {
    private val executor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("state-store-maintenance-task")

    private val runnable = new Runnable {
      override def run(): Unit = {
        try {
          task
        } catch {
          case NonFatal(e) =>
            logWarning("Error running maintenance thread", e)
            onError
            throw e
        }
      }
    }

    private val future: ScheduledFuture[_] = executor.scheduleAtFixedRate(
      runnable, periodMs, periodMs, TimeUnit.MILLISECONDS)

    def stop(): Unit = {
      future.cancel(false)
      executor.shutdown()
    }

    def isRunning: Boolean = !future.isDone
  }

  @GuardedBy("loadedProviders")
  private var maintenanceTask: MaintenanceTask = null

  @GuardedBy("loadedProviders")
  private var _coordRef: StateStoreCoordinatorRef = null

  /** Get or create a store associated with the id.
    * 获取或创建与ID相关联的存储区*/
  def get(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      indexOrdinal: Option[Int],
      version: Long,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStore = {
    require(version >= 0)
    val storeProvider = loadedProviders.synchronized {
      startMaintenanceIfNeeded()
      val provider = loadedProviders.getOrElseUpdate(
        storeProviderId,
        StateStoreProvider.createAndInit(
          storeProviderId.storeId, keySchema, valueSchema, indexOrdinal, storeConf, hadoopConf)
      )
      reportActiveStoreInstance(storeProviderId)
      provider
    }
    storeProvider.getStore(version)
  }

  /** Unload a state store provider
    * 卸载状态存储提供程序*/
  def unload(storeProviderId: StateStoreProviderId): Unit = loadedProviders.synchronized {
    loadedProviders.remove(storeProviderId).foreach(_.close())
  }

  /** Whether a state store provider is loaded or not
    * 是否已加载状态存储提供程序*/
  def isLoaded(storeProviderId: StateStoreProviderId): Boolean = loadedProviders.synchronized {
    loadedProviders.contains(storeProviderId)
  }

  def isMaintenanceRunning: Boolean = loadedProviders.synchronized {
    maintenanceTask != null && maintenanceTask.isRunning
  }

  /** Unload and stop all state store providers
    * 卸载并停止所有状态存储提供程序*/
  def stop(): Unit = loadedProviders.synchronized {
    loadedProviders.keySet.foreach { key => unload(key) }
    loadedProviders.clear()
    _coordRef = null
    if (maintenanceTask != null) {
      maintenanceTask.stop()
      maintenanceTask = null
    }
    logInfo("StateStore stopped")
  }

  /** Start the periodic maintenance task if not already started and if Spark active
    * 如果没有启动,如果有Spark活动,则启动定期维护任务。*/
  private def startMaintenanceIfNeeded(): Unit = loadedProviders.synchronized {
    val env = SparkEnv.get
    if (env != null && !isMaintenanceRunning) {
      val periodMs = env.conf.getTimeAsMs(
        MAINTENANCE_INTERVAL_CONFIG, s"${MAINTENANCE_INTERVAL_DEFAULT_SECS}s")
      maintenanceTask = new MaintenanceTask(
        periodMs,
        task = { doMaintenance() },
        onError = { loadedProviders.synchronized { loadedProviders.clear() } }
      )
      logInfo("State Store maintenance task started")
    }
  }

  /**
   * Execute background maintenance task in all the loaded store providers if they are still
   * the active instances according to the coordinator.
    * 根据协调器(如果仍然是活动实例)在所有加载的存储提供程序中执行后台维护任务
   */
  private def doMaintenance(): Unit = {
    logDebug("Doing maintenance")
    if (SparkEnv.get == null) {
      throw new IllegalStateException("SparkEnv not active, cannot do maintenance on StateStores")
    }
    loadedProviders.synchronized { loadedProviders.toSeq }.foreach { case (id, provider) =>
      try {
        if (verifyIfStoreInstanceActive(id)) {
          provider.doMaintenance()
        } else {
          unload(id)
          logInfo(s"Unloaded $provider")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"Error managing $provider, stopping management thread")
          throw e
      }
    }
  }

  private def reportActiveStoreInstance(storeProviderId: StateStoreProviderId): Unit = {
    if (SparkEnv.get != null) {
      val host = SparkEnv.get.blockManager.blockManagerId.host
      val executorId = SparkEnv.get.blockManager.blockManagerId.executorId
      coordinatorRef.foreach(_.reportActiveInstance(storeProviderId, host, executorId))
      logInfo(s"Reported that the loaded instance $storeProviderId is active")
    }
  }

  private def verifyIfStoreInstanceActive(storeProviderId: StateStoreProviderId): Boolean = {
    if (SparkEnv.get != null) {
      val executorId = SparkEnv.get.blockManager.blockManagerId.executorId
      val verified =
        coordinatorRef.map(_.verifyIfInstanceActive(storeProviderId, executorId)).getOrElse(false)
      logDebug(s"Verified whether the loaded instance $storeProviderId is active: $verified")
      verified
    } else {
      false
    }
  }

  private def coordinatorRef: Option[StateStoreCoordinatorRef] = loadedProviders.synchronized {
    val env = SparkEnv.get
    if (env != null) {
      logInfo("Env is not null")
      val isDriver =
        env.executorId == SparkContext.DRIVER_IDENTIFIER ||
          env.executorId == SparkContext.LEGACY_DRIVER_IDENTIFIER
      // If running locally, then the coordinator reference in _coordRef may be have become inactive
      // as SparkContext + SparkEnv may have been restarted. Hence, when running in driver,
      // always recreate the reference.
      if (isDriver || _coordRef == null) {
        logInfo("Getting StateStoreCoordinatorRef")
        _coordRef = StateStoreCoordinatorRef.forExecutor(env)
      }
      logInfo(s"Retrieved reference to StateStoreCoordinator: ${_coordRef}")
      Some(_coordRef)
    } else {
      logInfo("Env is null")
      _coordRef = null
      None
    }
  }
}

