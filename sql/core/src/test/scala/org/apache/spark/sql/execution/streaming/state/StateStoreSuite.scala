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

import java.io.{File, IOException}
import java.net.URI
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.LocalSparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
//状态存储套件
class StateStoreSuite extends StateStoreSuiteBase[HDFSBackedStateStoreProvider]
  with BeforeAndAfter with PrivateMethodTester {
  type MapType = mutable.HashMap[UnsafeRow, UnsafeRow]

  import StateStoreCoordinatorSuite._
  import StateStoreTestsHelper._

  val keySchema = StructType(Seq(StructField("key", StringType, true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }
  //快照
  test("snapshotting") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

    var currentVersion = 0
    def updateVersionTo(targetVersion: Int): Unit = {
      for (i <- currentVersion + 1 to targetVersion) {
        val store = provider.getStore(currentVersion)
        put(store, "a", i)
        store.commit()
        currentVersion += 1
      }
      require(currentVersion === targetVersion)
    }

    updateVersionTo(2)
    require(getData(provider) === Set("a" -> 2))
    //不应生成快照文件
    provider.doMaintenance()               // should not generate snapshot files
    assert(getData(provider) === Set("a" -> 2))

    for (i <- 1 to currentVersion) {
      //所有delta文件当前
      assert(fileExists(provider, i, isSnapshot = false))  // all delta files present
      //没有快照文件
      assert(!fileExists(provider, i, isSnapshot = true))  // no snapshot files present
    }

    // After version 6, snapshotting should generate one snapshot file
    //6版本后,快照应该生成一个快照文件
    updateVersionTo(6)
    require(getData(provider) === Set("a" -> 6), "store not updated correctly")
    provider.doMaintenance()       // should generate snapshot files

    val snapshotVersion = (0 to 6).find(version => fileExists(provider, version, isSnapshot = true))
    assert(snapshotVersion.nonEmpty, "snapshot file not generated")
    deleteFilesEarlierThanVersion(provider, snapshotVersion.get)
    assert(
      getData(provider, snapshotVersion.get) === Set("a" -> snapshotVersion.get),
      "snapshotting messed up the data of the snapshotted version")
    assert(
      getData(provider) === Set("a" -> 6),
      "snapshotting messed up the data of the final version")

    // After version 20, snapshotting should generate newer snapshot files
    //20版本后，快照应该生成新快照文件
    updateVersionTo(20)
    require(getData(provider) === Set("a" -> 20), "store not updated correctly")
    provider.doMaintenance()       // do snapshot

    val latestSnapshotVersion = (0 to 20).filter(version =>
      fileExists(provider, version, isSnapshot = true)).lastOption
    assert(latestSnapshotVersion.nonEmpty, "no snapshot file found")
    assert(latestSnapshotVersion.get > snapshotVersion.get, "newer snapshot not generated")

    deleteFilesEarlierThanVersion(provider, latestSnapshotVersion.get)
    assert(getData(provider) === Set("a" -> 20), "snapshotting messed up the data")
  }
  //清理
  test("cleaning") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)

    for (i <- 1 to 20) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    require(
      rowsToSet(provider.latestIterator()) === Set("a" -> 20),
      "store not updated correctly")

    assert(!fileExists(provider, version = 1, isSnapshot = false)) // first file should be deleted

    // last couple of versions should be retrievable
    //最后对版本可以还原
    assert(getData(provider, 20) === Set("a" -> 20))
    assert(getData(provider, 19) === Set("a" -> 19))
  }
  //在HDFS上提交增量文件不应该失败
  test("SPARK-19677: Committing a delta file atop an existing one should not fail on HDFS") {
    val conf = new Configuration()
    conf.set("fs.fake.impl", classOf[RenameLikeHDFSFileSystem].getName)
    conf.set("fs.defaultFS", "fake:///")

    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, hadoopConf = conf)
    provider.getStore(0).commit()
    provider.getStore(0).commit()

    // Verify we don't leak temp files
    //确认我们不会泄漏临时文件
    val tempFiles = FileUtils.listFiles(new File(provider.stateStoreId.checkpointRootLocation),
      null, true).asScala.filter(_.getName.startsWith("temp-"))
    assert(tempFiles.isEmpty)
  }
  //损坏的文件处理
  test("corrupted file handling") {
    val provider = newStoreProvider(opId = Random.nextInt, partition = 0, minDeltasForSnapshot = 5)
    for (i <- 1 to 6) {
      val store = provider.getStore(i - 1)
      put(store, "a", i)
      store.commit()
      provider.doMaintenance() // do cleanup
    }
    val snapshotVersion = (0 to 10).find( version =>
      fileExists(provider, version, isSnapshot = true)).getOrElse(fail("snapshot file not found"))

    // Corrupt snapshot file and verify that it throws error
    //损坏的快照文件并验证它是否引发错误
    assert(getData(provider, snapshotVersion) === Set("a" -> snapshotVersion))
    corruptFile(provider, snapshotVersion, isSnapshot = true)
    intercept[Exception] {
      getData(provider, snapshotVersion)
    }

    // Corrupt delta file and verify that it throws error
    //损坏的增量文件并验证它是否引发错误
    assert(getData(provider, snapshotVersion - 1) === Set("a" -> (snapshotVersion - 1)))
    corruptFile(provider, snapshotVersion - 1, isSnapshot = false)
    intercept[Exception] {
      getData(provider, snapshotVersion - 1)
    }

    // Delete delta file and verify that it throws error
    //删除增量文件并验证它是否引发错误
    deleteFilesEarlierThanVersion(provider, snapshotVersion)
    intercept[Exception] {
      getData(provider, snapshotVersion - 1)
    }
  }
  //报告内存使用情况
  test("reports memory usage") {
    val provider = newStoreProvider()
    val store = provider.getStore(0)
    val noDataMemoryUsed = store.metrics.memoryUsedBytes
    put(store, "a", 1)
    store.commit()
    assert(store.metrics.memoryUsedBytes > noDataMemoryUsed)
  }

  test("StateStore.get") {
    quietly {
      val dir = newDir()
      val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
      val storeConf = StateStoreConf.empty
      val hadoopConf = new Configuration()

      // Verify that trying to get incorrect versions throw errors
      //验证试图得到不正确的版本抛出错误
      intercept[IllegalArgumentException] {
        StateStore.get(
          storeId, keySchema, valueSchema, None, -1, storeConf, hadoopConf)
      }
      //版本1不应尝试加载存储区
      assert(!StateStore.isLoaded(storeId)) // version -1 should not attempt to load the store

      intercept[IllegalStateException] {
        StateStore.get(
          storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      }

      // Increase version of the store and try to get again
      //增加商店的版本，并尝试再次获得

      val store0 = StateStore.get(
        storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
      assert(store0.version === 0)
      put(store0, "a", 1)
      store0.commit()

      val store1 = StateStore.get(
        storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      assert(StateStore.isLoaded(storeId))
      assert(store1.version === 1)
      assert(rowsToSet(store1.iterator()) === Set("a" -> 1))

      // Verify that you can also load older version
      //验证您还可以加载旧版本
      val store0reloaded = StateStore.get(
        storeId, keySchema, valueSchema, None, 0, storeConf, hadoopConf)
      assert(store0reloaded.version === 0)
      assert(rowsToSet(store0reloaded.iterator()) === Set.empty)

      // Verify that you can remove the store and still reload and use it
      //验证您可以删除存储并继续重新加载并使用它
      StateStore.unload(storeId)
      assert(!StateStore.isLoaded(storeId))

      val store1reloaded = StateStore.get(
        storeId, keySchema, valueSchema, None, 1, storeConf, hadoopConf)
      assert(StateStore.isLoaded(storeId))
      assert(store1reloaded.version === 1)
      put(store1reloaded, "a", 2)
      assert(store1reloaded.commit() === 2)
      assert(rowsToSet(store1reloaded.iterator()) === Set("a" -> 2))
    }
  }
  //维修
  test("maintenance") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      // Make maintenance thread do snapshots and cleanups very fast
      //使维护线程做快照和清理的很快
      .set(StateStore.MAINTENANCE_INTERVAL_CONFIG, "10ms")
      // Make sure that when SparkContext stops, the StateStore maintenance thread 'quickly'
      // fails to talk to the StateStoreCoordinator and unloads all the StateStores
      .set("spark.rpc.numRetries", "1")
    val opId = 0
    val dir = newDir()
    val storeProviderId = StateStoreProviderId(StateStoreId(dir, opId, 0), UUID.randomUUID)
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    val storeConf = StateStoreConf(sqlConf)
    val hadoopConf = new Configuration()
    val provider = newStoreProvider(storeProviderId.storeId)

    var latestStoreVersion = 0

    def generateStoreVersions() {
      for (i <- 1 to 20) {
        val store = StateStore.get(storeProviderId, keySchema, valueSchema, None,
          latestStoreVersion, storeConf, hadoopConf)
        put(store, "a", i)
        store.commit()
        latestStoreVersion += 1
      }
    }

    val timeoutDuration = 60 seconds

    quietly {
      withSpark(new SparkContext(conf)) { sc =>
        withCoordinatorRef(sc) { coordinatorRef =>
          require(!StateStore.isMaintenanceRunning, "StateStore is unexpectedly running")

          // Generate sufficient versions of store for snapshots
          //为快照生成足够的存储版本
          generateStoreVersions()

          eventually(timeout(timeoutDuration)) {
            // Store should have been reported to the coordinator
            //存储应该向协调员报告。
            assert(coordinatorRef.getLocation(storeProviderId).nonEmpty,
              "active instance was not reported")

            // Background maintenance should clean up and generate snapshots
            //后台维护应该清理并生成快照
            assert(StateStore.isMaintenanceRunning, "Maintenance task is not running")

            // Some snapshots should have been generated
            //应该生成一些快照
            val snapshotVersions = (1 to latestStoreVersion).filter { version =>
              fileExists(provider, version, isSnapshot = true)
            }
            assert(snapshotVersions.nonEmpty, "no snapshot file found")
          }

          // Generate more versions such that there is another snapshot and
          // the earliest delta file will be cleaned up
          //生成更多的版本,以便有另一个快照,最早的delta文件将被清理
          generateStoreVersions()

          // Earliest delta file should get cleaned up
          //最早的delta文件应该清理干净
          eventually(timeout(timeoutDuration)) {
            assert(!fileExists(provider, 1, isSnapshot = false), "earliest file not deleted")
          }

          // If driver decides to deactivate all stores related to a query run,
          // then this instance should be unloaded
          //如果驱动程序决定停用与查询运行相关的所有存储,则应卸载此实例
          coordinatorRef.deactivateInstances(storeProviderId.queryRunId)
          eventually(timeout(timeoutDuration)) {
            assert(!StateStore.isLoaded(storeProviderId))
          }

          // Reload the store and verify 重新加载存储并验证
          StateStore.get(storeProviderId, keySchema, valueSchema, indexOrdinal = None,
            latestStoreVersion, storeConf, hadoopConf)
          assert(StateStore.isLoaded(storeProviderId))

          // If some other executor loads the store, then this instance should be unloaded
          //如果其他执行器加载该存储,则应卸载此实例。
          coordinatorRef.reportActiveInstance(storeProviderId, "other-host", "other-exec")
          eventually(timeout(timeoutDuration)) {
            assert(!StateStore.isLoaded(storeProviderId))
          }

          // Reload the store and verify 重新加载存储并验证
          StateStore.get(storeProviderId, keySchema, valueSchema, indexOrdinal = None,
            latestStoreVersion, storeConf, hadoopConf)
          assert(StateStore.isLoaded(storeProviderId))
        }
      }

      // Verify if instance is unloaded if SparkContext is stopped
      //如果停止实例验证sparkcontext卸载
      eventually(timeout(timeoutDuration)) {
        require(SparkEnv.get === null)
        assert(!StateStore.isLoaded(storeProviderId))
        assert(!StateStore.isMaintenanceRunning)
      }
    }
  }
  //重命名失败时提交失败
  test("SPARK-18342: commit fails when rename fails") {
    import RenameReturnsFalseFileSystem._
    val dir = scheme + "://" + newDir()
    val conf = new Configuration()
    conf.set(s"fs.$scheme.impl", classOf[RenameReturnsFalseFileSystem].getName)
    val provider = newStoreProvider(
      opId = Random.nextInt, partition = 0, dir = dir, hadoopConf = conf)
    val store = provider.getStore(0)
    put(store, "a", 0)
    val e = intercept[IllegalStateException](store.commit())
    assert(e.getCause.getMessage.contains("Failed to rename"))
  }
  //在更新存储之前,不要创建临时delta文件
  test("SPARK-18416: do not create temp delta file until the store is updated") {
    val dir = newDir()
    val storeId = StateStoreProviderId(StateStoreId(dir, 0, 0), UUID.randomUUID)
    val storeConf = StateStoreConf.empty
    val hadoopConf = new Configuration()
    val deltaFileDir = new File(s"$dir/0/0/")

    def numTempFiles: Int = {
      if (deltaFileDir.exists) {
        deltaFileDir.listFiles.map(_.getName).count(n => n.contains("temp") && !n.startsWith("."))
      } else 0
    }

    def numDeltaFiles: Int = {
      if (deltaFileDir.exists) {
        deltaFileDir.listFiles.map(_.getName).count(n => n.contains(".delta") && !n.startsWith("."))
      } else 0
    }

    def shouldNotCreateTempFile[T](body: => T): T = {
      val before = numTempFiles
      val result = body
      assert(numTempFiles === before)
      result
    }

    // Getting the store should not create temp file
    //获取存储不应该创建临时文件
    val store0 = shouldNotCreateTempFile {
      StateStore.get(
        storeId, keySchema, valueSchema, indexOrdinal = None, version = 0, storeConf, hadoopConf)
    }

    // Put should create a temp file
    //应该创建一个临时文件
    put(store0, "a", 1)
    assert(numTempFiles === 1)
    assert(numDeltaFiles === 0)

    // Commit should remove temp file and create a delta file
    //提交应该删除临时文件并创建一个增量文件
    store0.commit()
    assert(numTempFiles === 0)
    assert(numDeltaFiles === 1)

    // Remove should create a temp file
    //删除应该创建一个临时文件
    val store1 = shouldNotCreateTempFile {
      StateStore.get(
        storeId, keySchema, valueSchema, indexOrdinal = None, version = 1, storeConf, hadoopConf)
    }
    remove(store1, _ == "a")
    assert(numTempFiles === 1)
    assert(numDeltaFiles === 1)

    // Commit should remove temp file and create a delta file
    //提交应该删除临时文件并创建一个增量文件
    store1.commit()
    assert(numTempFiles === 0)
    assert(numDeltaFiles === 2)

    // Commit without any updates should create a delta file
    //没有任何更新的提交应该创建一个增量文件
    val store2 = shouldNotCreateTempFile {
      StateStore.get(
        storeId, keySchema, valueSchema, indexOrdinal = None, version = 2, storeConf, hadoopConf)
    }
    store2.commit()
    assert(numTempFiles === 0)
    assert(numDeltaFiles === 3)
  }
  //重新启动查询创建新的提供程序实例
  test("SPARK-21145: Restarted queries create new provider instances") {
    try {
      val checkpointLocation = Utils.createTempDir().getAbsoluteFile
      val spark = SparkSession.builder().master("local[2]").getOrCreate()
      SparkSession.setActiveSession(spark)
      implicit val sqlContext = spark.sqlContext
      spark.conf.set("spark.sql.shuffle.partitions", "1")
      import spark.implicits._
      val inputData = MemoryStream[Int]

      def runQueryAndGetLoadedProviders(): Seq[StateStoreProvider] = {
        val aggregated = inputData.toDF().groupBy("value").agg(count("*"))
        // stateful query 状态查询
        val query = aggregated.writeStream
          // 输出接收器 内存接收器（用于调试） - 输出作为内存表存储在内存中。支持附加和完成输出模式。
          // 这应该用于低数据量上的调试目的，因为每次触发后，整个输出被收集并存储在驱动程序的内存中。
          .format("memory")
          //"Output"是写入到外部存储的写方式,
          //Complete Mode 将整个更新表写入到外部存储,写入整个表的方式由存储连接器决定
          .outputMode("complete")
          .queryName("query")
          .option("checkpointLocation", checkpointLocation.toString)
          .start()
        inputData.addData(1, 2, 3)
        query.processAllAvailable()
        //启动后至少有一批处理
        require(query.lastProgress != null) // at least one batch processed after start
        val loadedProvidersMethod =
          PrivateMethod[mutable.HashMap[StateStoreProviderId, StateStoreProvider]]('loadedProviders)
        val loadedProvidersMap = StateStore invokePrivate loadedProvidersMethod()
        val loadedProviders = loadedProvidersMap.synchronized { loadedProvidersMap.values.toSeq }
        query.stop()
        loadedProviders
      }

      val loadedProvidersAfterRun1 = runQueryAndGetLoadedProviders()
      require(loadedProvidersAfterRun1.length === 1)

      val loadedProvidersAfterRun2 = runQueryAndGetLoadedProviders()
      //两个提供2次运行的提供者
      assert(loadedProvidersAfterRun2.length === 2)   // two providers loaded for 2 runs

      // Both providers should have the same StateStoreId, but the should be different objects
      //两个提供者应该有相同的StateStoreId，但是应该是不同的对象
      assert(loadedProvidersAfterRun2(0).stateStoreId === loadedProvidersAfterRun2(1).stateStoreId)
      assert(loadedProvidersAfterRun2(0) ne loadedProvidersAfterRun2(1))

    } finally {
      SparkSession.getActiveSession.foreach { spark =>
        spark.streams.active.foreach(_.stop())
        spark.stop()
      }
    }
  }

  override def newStoreProvider(): HDFSBackedStateStoreProvider = {
    newStoreProvider(opId = Random.nextInt(), partition = 0)
  }

  override def newStoreProvider(storeId: StateStoreId): HDFSBackedStateStoreProvider = {
    newStoreProvider(storeId.operatorId, storeId.partitionId, dir = storeId.checkpointRootLocation)
  }

  override def getLatestData(storeProvider: HDFSBackedStateStoreProvider): Set[(String, Int)] = {
    getData(storeProvider)
  }

  override def getData(
    provider: HDFSBackedStateStoreProvider,
    version: Int = -1): Set[(String, Int)] = {
    val reloadedProvider = newStoreProvider(provider.stateStoreId)
    if (version < 0) {
      reloadedProvider.latestIterator().map(rowsToStringInt).toSet
    } else {
      reloadedProvider.getStore(version).iterator().map(rowsToStringInt).toSet
    }
  }

  def newStoreProvider(
      opId: Long,
      partition: Int,
      dir: String = newDir(),
      minDeltasForSnapshot: Int = SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      hadoopConf: Configuration = new Configuration): HDFSBackedStateStoreProvider = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    val provider = new HDFSBackedStateStoreProvider()
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      indexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }

  def fileExists(
      provider: HDFSBackedStateStoreProvider,
      version: Long,
      isSnapshot: Boolean): Boolean = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.exists
  }

  def deleteFilesEarlierThanVersion(provider: HDFSBackedStateStoreProvider, version: Long): Unit = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    for (version <- 0 until version.toInt) {
      for (isSnapshot <- Seq(false, true)) {
        val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
        val filePath = new File(basePath.toString, fileName)
        if (filePath.exists) filePath.delete()
      }
    }
  }

  def corruptFile(
    provider: HDFSBackedStateStoreProvider,
    version: Long,
    isSnapshot: Boolean): Unit = {
    val method = PrivateMethod[Path]('baseDir)
    val basePath = provider invokePrivate method()
    val fileName = if (isSnapshot) s"$version.snapshot" else s"$version.delta"
    val filePath = new File(basePath.toString, fileName)
    filePath.delete()
    filePath.createNewFile()
  }
}

abstract class StateStoreSuiteBase[ProviderClass <: StateStoreProvider]
  extends SparkFunSuite {
  import StateStoreTestsHelper._

  test("get, put, remove, commit, and all data iterator") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    //在开始一组新的更新之前验证状态
    assert(getLatestData(provider).isEmpty)

    val store = provider.getStore(0)
    assert(!store.hasCommitted)
    assert(get(store, "a") === None)
    assert(store.iterator().isEmpty)
    assert(store.metrics.numKeys === 0)

    // Verify state after updating
    //更新后验证状态
    put(store, "a", 1)
    assert(get(store, "a") === Some(1))
    assert(store.metrics.numKeys === 1)

    assert(store.iterator().nonEmpty)
    assert(getLatestData(provider).isEmpty)

    // Make updates, commit and then verify state
    //进行更新,提交然后验证状态
    put(store, "b", 2)
    put(store, "aa", 3)
    assert(store.metrics.numKeys === 3)
    remove(store, _.startsWith("a"))
    assert(store.metrics.numKeys === 1)
    assert(store.commit() === 1)

    assert(store.hasCommitted)
    assert(rowsToSet(store.iterator()) === Set("b" -> 2))
    assert(getLatestData(provider) === Set("b" -> 2))

    // Trying to get newer versions should fail
    //试图获得更新的版本应该失败
    intercept[Exception] {
      provider.getStore(2)
    }
    intercept[Exception] {
      getData(provider, 2)
    }

    // New updates to the reloaded store with new version, and does not change old version
    //重新装载新版本的新版本,不会更改旧版本
    val reloadedProvider = newStoreProvider(store.id)
    val reloadedStore = reloadedProvider.getStore(1)
    assert(reloadedStore.metrics.numKeys === 1)
    put(reloadedStore, "c", 4)
    assert(reloadedStore.metrics.numKeys === 2)
    assert(reloadedStore.commit() === 2)
    assert(rowsToSet(reloadedStore.iterator()) === Set("b" -> 2, "c" -> 4))
    assert(getLatestData(provider) === Set("b" -> 2, "c" -> 4))
    assert(getData(provider, version = 1) === Set("b" -> 2))
  }
  //在迭代时删除
  test("removing while iterating") {
    val provider = newStoreProvider()

    // Verify state before starting a new set of updates
    //在开始一组新的更新之前验证状态
    assert(getLatestData(provider).isEmpty)
    val store = provider.getStore(0)
    put(store, "a", 1)
    put(store, "b", 2)

    // Updates should work while iterating of filtered entries
    //更新应该在迭代过滤条目时工作
    val filtered = store.iterator.filter { tuple => rowToString(tuple.key) == "a" }
    filtered.foreach { tuple =>
      store.put(tuple.key, intToRow(rowToInt(tuple.value) + 1))
    }
    assert(get(store, "a") === Some(2))

    // Removes should work while iterating of filtered entries
    //删除应该在迭代过滤的条目时工作
    val filtered2 = store.iterator.filter { tuple => rowToString(tuple.key) == "b" }
    filtered2.foreach { tuple => store.remove(tuple.key) }
    assert(get(store, "b") === None)
  }
  //退出
  test("abort") {
    val provider = newStoreProvider()
    val store = provider.getStore(0)
    put(store, "a", 1)
    store.commit()
    assert(rowsToSet(store.iterator()) === Set("a" -> 1))

    // cancelUpdates should not change the data in the files
    //cancelUpdates不应该更改文件中的数据
    val store1 = provider.getStore(1)
    put(store1, "b", 1)
    store1.abort()
  }
  //getStore with invalid versions
  test("getStore with invalid versions") {
    val provider = newStoreProvider()

    def checkInvalidVersion(version: Int): Unit = {
      intercept[Exception] {
        provider.getStore(version)
      }
    }

    checkInvalidVersion(-1)
    checkInvalidVersion(1)

    val store = provider.getStore(0)
    put(store, "a", 1)
    assert(store.commit() === 1)
    assert(rowsToSet(store.iterator()) === Set("a" -> 1))

    val store1_ = provider.getStore(1)
    assert(rowsToSet(store1_.iterator()) === Set("a" -> 1))

    checkInvalidVersion(-1)
    checkInvalidVersion(2)

    // Update store version with some data
    val store1 = provider.getStore(1)
    assert(rowsToSet(store1.iterator()) === Set("a" -> 1))
    put(store1, "b", 1)
    assert(store1.commit() === 2)
    assert(rowsToSet(store1.iterator()) === Set("a" -> 1, "b" -> 1))

    checkInvalidVersion(-1)
    checkInvalidVersion(3)
  }
  //两个并发的StateStore - 一个用于只读，一个用于读写
  test("two concurrent StateStores - one for read-only and one for read-write") {
    // During Streaming Aggregation, we have two StateStores per task, one used as read-only in
    // `StateStoreRestoreExec`, and one read-write used in `StateStoreSaveExec`. `StateStore.abort`
    // will be called for these StateStores if they haven't committed their results. We need to
    // make sure that `abort` in read-only store after a `commit` in the read-write store doesn't
    // accidentally lead to the deletion of state.
    //在Streaming Aggregation期间,每个任务有两个StateStore,一个在StateStoreRestoreExec中只读,一个在StateStoreSaveExec中使用。
    // 如果这些StateStore没有提交结果,将会调用StateStore.abort
    // 我们需要确保在读写存储中的`commit`后的只读存储中的`abort`不会意外地导致状态的删除。
    val dir = newDir()
    val storeId = StateStoreId(dir, 0L, 1)
    val provider0 = newStoreProvider(storeId)
    // prime state 主要状态
    val store = provider0.getStore(0)
    val key = "a"
    put(store, key, 1)
    store.commit()
    assert(rowsToSet(store.iterator()) === Set(key -> 1))

    // two state stores 两状态存储
    val provider1 = newStoreProvider(storeId)
    val restoreStore = provider1.getStore(1)
    val saveStore = provider1.getStore(1)

    put(saveStore, key, get(restoreStore, key).get + 1)
    saveStore.commit()
    restoreStore.abort()

    // check that state is correct for next batch
    //检查下一批次的状态是否正确
    val provider2 = newStoreProvider(storeId)
    val finalStore = provider2.getStore(2)
    assert(rowsToSet(finalStore.iterator()) === Set(key -> 2))
  }

  /** Return a new provider with a random id
    * 用随机id返回一个新的提供者*/
  def newStoreProvider(): ProviderClass

  /** Return a new provider with the given id
    * 返回给定ID的新提供者*/
  def newStoreProvider(storeId: StateStoreId): ProviderClass

  /** Get the latest data referred to by the given provider but not using this provider
    * 获取给定提供者引用的最新数据,但不使用此提供程序。*/
  def getLatestData(storeProvider: ProviderClass): Set[(String, Int)]

  /**
   * Get a specific version of data referred to by the given provider but not using
   * this provider
    * 获取给定提供者引用的数据的特定版本,但不使用此提供程序
   */
  def getData(storeProvider: ProviderClass, version: Int): Set[(String, Int)]
}

object StateStoreTestsHelper {

  val strProj = UnsafeProjection.create(Array[DataType](StringType))
  val intProj = UnsafeProjection.create(Array[DataType](IntegerType))

  def stringToRow(s: String): UnsafeRow = {
    strProj.apply(new GenericInternalRow(Array[Any](UTF8String.fromString(s)))).copy()
  }

  def intToRow(i: Int): UnsafeRow = {
    intProj.apply(new GenericInternalRow(Array[Any](i))).copy()
  }

  def rowToString(row: UnsafeRow): String = {
    row.getUTF8String(0).toString
  }

  def rowToInt(row: UnsafeRow): Int = {
    row.getInt(0)
  }

  def rowsToStringInt(row: UnsafeRowPair): (String, Int) = {
    (rowToString(row.key), rowToInt(row.value))
  }

  def rowsToSet(iterator: Iterator[UnsafeRowPair]): Set[(String, Int)] = {
    iterator.map(rowsToStringInt).toSet
  }

  def remove(store: StateStore, condition: String => Boolean): Unit = {
    store.getRange(None, None).foreach { rowPair =>
      if (condition(rowToString(rowPair.key))) store.remove(rowPair.key)
    }
  }

  def put(store: StateStore, key: String, value: Int): Unit = {
    store.put(stringToRow(key), intToRow(value))
  }

  def get(store: StateStore, key: String): Option[Int] = {
    Option(store.get(stringToRow(key))).map(rowToInt)
  }

  def newDir(): String = Utils.createTempDir().toString
}

/**
 * Fake FileSystem that simulates HDFS rename semantic, i.e. renaming a file atop an existing
 * one should return false.
  * 假文件系统HDFS重命名重命名模拟语义,即在现有的一个文件应该返回false
 * See hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
 */
class RenameLikeHDFSFileSystem extends RawLocalFileSystem {
  override def rename(src: Path, dst: Path): Boolean = {
    if (exists(dst)) {
      return false
    } else {
      return super.rename(src, dst)
    }
  }
}

/**
 * Fake FileSystem to test that the StateStore throws an exception while committing the
 * delta file, when `fs.rename` returns `false`.
  * 假FileSystem测试StateStore在提交delta文件时抛出一个异常,当`fs.rename`返回`false`时
 */
class RenameReturnsFalseFileSystem extends RawLocalFileSystem {
  import RenameReturnsFalseFileSystem._
  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def rename(src: Path, dst: Path): Boolean = false
}

object RenameReturnsFalseFileSystem {
  val scheme = s"StateStoreSuite${math.abs(Random.nextInt)}fs"
}
