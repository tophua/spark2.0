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

import java.io.{InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.UUID
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.command.StreamingExplainCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.util.{Clock, UninterruptibleThread, Utils}

/** States for [[StreamExecution]]'s lifecycle.
  * 针对[[StreamExecution]]的生命周期*/
trait State
case object INITIALIZING extends State
case object ACTIVE extends State
case object TERMINATED extends State

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
  * 管理在独立线程中发生的流式Spark SQL查询的执行
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
  * 与标准查询不同,每当新数据到达查询计划中的[[Source]]时，就会重复执行流式查询,
  * 每当新数据到达时,[QueryExecution]创建并且结果被事务性地提交到给定的[[Sink]]
 *
 * @param deleteCheckpointOnStop whether to delete the checkpoint if the query is stopped without
 *                               errors
  *                               如果查询停止没有错误,是否删除检查点
 */
class StreamExecution(
    override val sparkSession: SparkSession,
    override val name: String,
    private val checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    val sink: Sink,
    val trigger: Trigger,
    val triggerClock: Clock,
    val outputMode: OutputMode,
    deleteCheckpointOnStop: Boolean)
  extends StreamingQuery with ProgressReporter with Logging {

  import org.apache.spark.sql.streaming.StreamingQueryListener._

  private val pollingDelayMs = sparkSession.sessionState.conf.streamingPollingDelay

  private val minBatchesToRetain = sparkSession.sessionState.conf.minBatchesToRetain
  require(minBatchesToRetain > 0, "minBatchesToRetain has to be positive")

  /**
   * A lock used to wait/notify when batches complete. Use a fair lock to avoid thread starvation.
    * 当批次完成时,用于等待/通知的锁,使用公平的锁,以避免线程饥饿
   */
  private val awaitBatchLock = new ReentrantLock(true)
  private val awaitBatchLockCondition = awaitBatchLock.newCondition()

  private val initializationLatch = new CountDownLatch(1)
  private val startLatch = new CountDownLatch(1)
  private val terminationLatch = new CountDownLatch(1)

  val resolvedCheckpointRoot = {
    val checkpointPath = new Path(checkpointRoot)
    val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    checkpointPath.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toUri.toString
  }

  /**
   * Tracks how much data we have processed and committed to the sink or state store from each
   * input source.
    * 跟踪我们已经处理了多少数据,并从每个输入源提交到接收器或状态存储器
   * Only the scheduler thread should modify this field, and only in atomic steps.
    * 只有调度程序线程应修改此字段,并且只能以原子步骤进行修改
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
    * 其他的线程如果要多次访问这个字段,就应该做一个浅拷贝,因为这个字段的值可能会随时改变
   */
  @volatile
  var committedOffsets = new StreamProgress

  /**
   * Tracks the offsets that are available to be processed, but have not yet be committed to the
   * sink.
    * 跟踪可用于处理的偏移量,但尚未提交给接收器
   * Only the scheduler thread should modify this field, and only in atomic steps.
    * 只有调度程序线程应修改此字段,并且只能以原子步骤进行修改。
   * Other threads should make a shallow copy if they are going to access this field more than
   * once, since the field's value may change at any time.
    * 其他的线程如果要多次访问这个字段,就应该做一个浅拷贝,因为这个字段的值可能会随时改变
   */
  @volatile
  var availableOffsets = new StreamProgress

  /** The current batchId or -1 if execution has not yet been initialized.
    * 当前的批次标识或-1（如果执行尚未初始化） */
  protected var currentBatchId: Long = -1

  /** Metadata associated with the whole query
    * 与整个查询关联的元数据*/
  protected val streamMetadata: StreamMetadata = {
    val metadataPath = new Path(checkpointFile("metadata"))
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    StreamMetadata.read(metadataPath, hadoopConf).getOrElse {
      val newMetadata = new StreamMetadata(UUID.randomUUID.toString)
      StreamMetadata.write(newMetadata, metadataPath, hadoopConf)
      newMetadata
    }
  }

  /** Metadata associated with the offset seq of a batch in the query.
    * 元数据与查询中批次的偏移量seq相关联*/
  protected var offsetSeqMetadata = OffsetSeqMetadata(
    batchWatermarkMs = 0, batchTimestampMs = 0, sparkSession.conf)

  /**
   * A map of current watermarks, keyed by the position of the watermark operator in the
   * physical plan.
   * 当前水印的地图,由水印操作员在实际计划中的位置锁定
   * This state is 'soft state', which does not affect the correctness and semantics of watermarks
   * and is not persisted across query restarts.
    * 这种状态是“软状态”,不会影响水印的正确性和语义,并且在查询重新启动时不会持续存在。
   * The fault-tolerant watermark state is in offsetSeqMetadata.
    * 容错水印状态位于offsetSeqMetadata中
   */
  protected val watermarkMsMap: MutableMap[Int, Long] = MutableMap()

  override val id: UUID = UUID.fromString(streamMetadata.id)

  override val runId: UUID = UUID.randomUUID

  /**
   * Pretty identified string of printing in logs. Format is
   * If name is set "queryName [id = xyz, runId = abc]" else "[id = xyz, runId = abc]"
    * 漂亮的日志打印字符串,格式是如果名称被设置为“queryName [id = xyz，runId = abc]”else“[id = xyz，runId = abc]”
   */
  private val prettyIdString =
    Option(name).map(_ + " ").getOrElse("") + s"[id = $id, runId = $runId]"

  /**
   * All stream sources present in the query plan. This will be set when generating logical plan.
    * 查询计划中存在的所有流源,这将在生成逻辑计划时设置
   */
  @volatile protected var sources: Seq[Source] = Seq.empty

  /**
   * A list of unique sources in the query plan. This will be set when generating logical plan.
    * 查询计划中唯一源的列表,这将在生成逻辑计划时设置
   */
  @volatile private var uniqueSources: Seq[Source] = Seq.empty

  override lazy val logicalPlan: LogicalPlan = {
    assert(microBatchThread eq Thread.currentThread,
      "logicalPlan must be initialized in StreamExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[StreamingRelation, StreamingExecutionRelation]()
    val _logicalPlan = analyzedPlan.transform {
      case streamingRelation@StreamingRelation(dataSource, _, output) =>
        toExecutionRelationMap.getOrElseUpdate(streamingRelation, {
          // Materialize source to avoid creating it in every batch
          //实现源代码以避免在每个批次中创建它
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSource.createSource(metadataPath)
          nextSourceId += 1
          // We still need to use the previous `output` instead of `source.schema` as attributes in
          // "df.logicalPlan" has already used attributes of the previous `output`.
          //我们仍然需要使用之前的`output`而不是`source.schema`作为“df.logicalPlan”中的属性已经使用了前一个`output`的属性。
          StreamingExecutionRelation(source, output)(sparkSession)
        })
    }
    sources = _logicalPlan.collect { case s: StreamingExecutionRelation => s.source }
    uniqueSources = sources.distinct
    _logicalPlan
  }

  private val triggerExecutor = trigger match {
    case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => OneTimeExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger")
  }

  /** Defines the internal state of execution
    * 定义执行的内部状态*/
  private val state = new AtomicReference[State](INITIALIZING)

  @volatile
  var lastExecution: IncrementalExecution = _

  /** Holds the most recent input data for each source.
    * 保存每个源的最新输入数据*/
  protected var newData: Map[Source, DataFrame] = _

  @volatile
  private var streamDeathCause: StreamingQueryException = null

  /* Get the call site in the caller thread; will pass this into the micro batch thread
   * 在调用者线程中获取调用网站; 将通过这个微批处理线程 */
  private val callSite = Utils.getCallSite()

  /** Used to report metrics to coda-hale. This uses id for easier tracking across restarts.
    * 用于向度量结果报告度量标准,这使用ID更容易跟踪重新启动*/
  lazy val streamMetrics = new MetricsReporter(
    this, s"spark.streaming.${Option(name).getOrElse(id)}")

  /**
   * The thread that runs the micro-batches of this stream. Note that this thread must be
   * [[org.apache.spark.util.UninterruptibleThread]] to workaround KAFKA-1894: interrupting a
   * running `KafkaConsumer` may cause endless loop.
    * 运行此流的微批处理的线程,请注意,此线程必须是[[org.apache.spark.util.UninterruptibleThread]]才能解决KAFKA-1894：
    * 中断正在运行的`KafkaConsumer`可能会导致无限循环
   */
  val microBatchThread =
    new StreamExecutionThread(s"stream execution thread for $prettyIdString") {
      override def run(): Unit = {
        // To fix call site like "run at <unknown>:0", we bridge the call site from the caller
        // thread to this micro batch thread
        //为了修复像“run at <unknown>：0”那样的呼叫站点，我们将呼叫站点从呼叫者线程桥接到这个微批处理线程
        sparkSession.sparkContext.setCallSite(callSite)
        runBatches()
      }
    }

  /**
   * A write-ahead-log that records the offsets that are present in each batch. In order to ensure
   * that a given batch will always consist of the same data, we write to this log *before* any
   * processing is done.  Thus, the Nth record in this log indicated data that is currently being
   * processed and the N-1th entry indicates which offsets have been durably committed to the sink.
    * 提前记录日志,记录每个批次中存在的偏移量,为了确保给定的批次总是由相同的数据组成,
    * 我们在完成任何处理之前写入这个日志*。 因此,该日志中的第N个记录指示了当前正在处理的数据,
    * 并且第N-1个条目指示了哪个偏移已经持续地提交给接收器。
   */
  val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile("offsets"))

  /**
   * A log that records the batch ids that have completed. This is used to check if a batch was
   * fully processed, and its output was committed to the sink, hence no need to process it again.
   * This is used (for instance) during restart, to help identify which batch to run next.
    * 记录已完成的批处理ID的日志,这是用来检查批处理是否完全处理,并且其输出被提交给接收器,因此不需要再次处理它,
    * 这在重启过程中（例如）用于帮助确定下一个要运行的批次。
   */
  val batchCommitLog = new BatchCommitLog(sparkSession, checkpointFile("commits"))

  /** Whether all fields of the query have been initialized
    * 查询的所有字段是否已经初始化*/
  private def isInitialized: Boolean = state.get != INITIALIZING

  /** Whether the query is currently active or not
    * 查询是否当前有效*/
  override def isActive: Boolean = state.get != TERMINATED

  /** Returns the [[StreamingQueryException]] if the query was terminated by an exception.
    * 如果查询被异常终止，则返回[[StreamingQueryException]]*/
  override def exception: Option[StreamingQueryException] = Option(streamDeathCause)

  /** Returns the path of a file with `name` in the checkpoint directory.
    * 在检查点目录中返回具有`name`的文件的路径。*/
  private def checkpointFile(name: String): String =
    new Path(new Path(resolvedCheckpointRoot), name).toUri.toString

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStartedEvent]]
   * has been posted to all the listeners.
    * 开始执行,这仅在线程启动并且[[QueryStartedEvent]]已经发布给所有监听者之后才返回
   */
  def start(): Unit = {
    logInfo(s"Starting $prettyIdString. Use $resolvedCheckpointRoot to store the query checkpoint.")
    microBatchThread.setDaemon(true)
    microBatchThread.start()
    //等待线程启动并发布QueryStart事件
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   * 反复尝试在数据到达时运行批处理
   * Note that this method ensures that [[QueryStartedEvent]] and [[QueryTerminatedEvent]] are
   * posted such that listeners are guaranteed to get a start event before a termination.
    * 请注意,此方法可确保发布[[QueryStartedEvent]]和[[QueryTerminatedEvent]],以确保侦听器在终止之前获得启动事件。
   * Furthermore, this method also ensures that [[QueryStartedEvent]] event is posted before the
   * `start()` method returns.
    * 此外，此方法还确保在[start（）方法返回之前发布[[QueryStartedEvent]]事件
   */
  private def runBatches(): Unit = {
    try {
      sparkSession.sparkContext.setJobGroup(runId.toString, getBatchDescriptionString,
        interruptOnCancel = true)
      sparkSession.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, id.toString)
      if (sparkSession.sessionState.conf.streamingMetricsEnabled) {
        sparkSession.sparkContext.env.metricsSystem.registerSource(streamMetrics)
      }

      // `postEvent` does not throw non fatal exception.
      //`postEvent`不会抛出非致命的异常
      postEvent(new QueryStartedEvent(id, runId, name))

      // Unblock starting thread
      //解除阻止开始的线程
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      //在激活时,反复尝试运行批次。
      SparkSession.setActiveSession(sparkSession)

      updateStatusMessage("Initializing sources")
      // force initialization of the logical plan so that the sources can be created
      //强制逻辑计划的初始化.以便创建源代码
      logicalPlan

      // Isolated spark session to run the batches with.
      //隔离的spark会话来运行批处理
      val sparkSessionToRunBatches = sparkSession.cloneSession()
      // Adaptive execution can change num shuffle partitions, disallow
      //自适应执行可以更改num个随机分区,禁止
      sparkSessionToRunBatches.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      offsetSeqMetadata = OffsetSeqMetadata(
        batchWatermarkMs = 0, batchTimestampMs = 0, sparkSessionToRunBatches.conf)

      if (state.compareAndSet(INITIALIZING, ACTIVE)) {
        // Unblock `awaitInitialization`
        //取消阻止“awaitInitialization”
        initializationLatch.countDown()

        triggerExecutor.execute(() => {
          startTrigger()

          if (isActive) {
            reportTimeTaken("triggerExecution") {
              if (currentBatchId < 0) {
                // We'll do this initialization only once
                //我们只做一次这个初始化
                populateStartOffsets(sparkSessionToRunBatches)
                sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)
                logDebug(s"Stream running from $committedOffsets to $availableOffsets")
              } else {
                constructNextBatch()
              }
              if (dataAvailable) {
                currentStatus = currentStatus.copy(isDataAvailable = true)
                updateStatusMessage("Processing new data")
                runBatch(sparkSessionToRunBatches)
              }
            }
            // Report trigger as finished and construct progress object.
            //报告触发器完成并构建进度对象
            finishTrigger(dataAvailable)
            if (dataAvailable) {
              // Update committed offsets.
              batchCommitLog.add(currentBatchId)
              committedOffsets ++= availableOffsets
              logDebug(s"batch ${currentBatchId} committed")
              // We'll increase currentBatchId after we complete processing current batch's data
              //完成当前批次数据的处理后,我们将增加currentBatchId
              currentBatchId += 1
              sparkSession.sparkContext.setJobDescription(getBatchDescriptionString)
            } else {
              currentStatus = currentStatus.copy(isDataAvailable = false)
              updateStatusMessage("Waiting for data to arrive")
              Thread.sleep(pollingDelayMs)
            }
          }
          updateStatusMessage("Waiting for next trigger")
          isActive
        })
        updateStatusMessage("Stopped")
      } else {
        // `stop()` is already called. Let `finally` finish the cleanup.
      }
    } catch {
      case e if isInterruptedByStop(e) =>
        // interrupted by stop() 被stop（）中断
        updateStatusMessage("Stopped")
      case e: IOException if e.getMessage != null
        && e.getMessage.startsWith(classOf[InterruptedException].getName)
        && state.get == TERMINATED =>
        // This is a workaround for HADOOP-12074: `Shell.runCommand` converts `InterruptedException`
        // to `new IOException(ie.toString())` before Hadoop 2.8.
        updateStatusMessage("Stopped")
      case e: Throwable =>
        streamDeathCause = new StreamingQueryException(
          toDebugString(includeLogicalPlan = isInitialized),
          s"Query $prettyIdString terminated with exception: ${e.getMessage}",
          e,
          committedOffsets.toOffsetSeq(sources, offsetSeqMetadata).toString,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata).toString)
        logError(s"Query $prettyIdString terminated with error", e)
        updateStatusMessage(s"Terminated with exception: ${e.getMessage}")
        // Rethrow the fatal errors to allow the user using `Thread.UncaughtExceptionHandler` to
        // handle them
        //重新发现致命的错误,以允许用户使用Thread.UncaughtExceptionHandler来处理它们
        if (!NonFatal(e)) {
          throw e
        }
    } finally microBatchThread.runUninterruptibly {
      // The whole `finally` block must run inside `runUninterruptibly` to avoid being interrupted
      // when a query is stopped by the user. We need to make sure the following codes finish
      // otherwise it may throw `InterruptedException` to `UncaughtExceptionHandler` (SPARK-21248).

      // Release latches to unblock the user codes since exception can happen in any place and we
      // may not get a chance to release them
      //释放锁存器以解锁用户代码,因为异常可能发生在任何地方,我们可能没有机会释放它们
      startLatch.countDown()
      initializationLatch.countDown()

      try {
        stopSources()
        state.set(TERMINATED)
        currentStatus = status.copy(isTriggerActive = false, isDataAvailable = false)

        // Update metrics and status 更新指标和状态
        sparkSession.sparkContext.env.metricsSystem.removeSource(streamMetrics)

        // Notify others 通知其他人
        sparkSession.streams.notifyQueryTermination(StreamExecution.this)
        postEvent(
          new QueryTerminatedEvent(id, runId, exception.map(_.cause).map(Utils.exceptionString)))

        // Delete the temp checkpoint only when the query didn't fail
        //只有当查询没有失败时才删除临时检查点
        if (deleteCheckpointOnStop && exception.isEmpty) {
          val checkpointPath = new Path(resolvedCheckpointRoot)
          try {
            val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
            fs.delete(checkpointPath, true)
          } catch {
            case NonFatal(e) =>
              // Deleting temp checkpoint folder is best effort, don't throw non fatal exceptions
              // when we cannot delete them.
              //删除临时检查点文件夹是尽力而为,不要在非法删除它们时抛出非致命异常
              logWarning(s"Cannot delete $checkpointPath", e)
          }
        }
      } finally {
        awaitBatchLock.lock()
        try {
          // Wake up any threads that are waiting for the stream to progress.
          //唤醒正在等待流进展的线程
          awaitBatchLockCondition.signalAll()
        } finally {
          awaitBatchLock.unlock()
        }
        terminationLatch.countDown()
      }
    }
  }

  private def isInterruptedByStop(e: Throwable): Boolean = {
    if (state.get == TERMINATED) {
      e match {
        // InterruptedIOException - thrown when an I/O operation is interrupted
          //InterruptedIOException - I / O操作中断时抛出
        // ClosedByInterruptException - thrown when an I/O operation upon a channel is interrupted
        case _: InterruptedException | _: InterruptedIOException | _: ClosedByInterruptException =>
          true
        // The cause of the following exceptions may be one of the above exceptions:
        //
        // UncheckedIOException - thrown by codes that cannot throw a checked IOException, such as
        //                        BiFunction.apply
        // ExecutionException - thrown by codes running in a thread pool and these codes throw an
        //                      exception
        // UncheckedExecutionException - thrown by codes that cannot throw a checked
        //                               ExecutionException, such as BiFunction.apply
        case e2 @ (_: UncheckedIOException | _: ExecutionException | _: UncheckedExecutionException)
          if e2.getCause != null =>
          isInterruptedByStop(e2.getCause)
        case _ =>
          false
      }
    } else {
      false
    }
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
    * 填充起始偏移量,以存储在接收器中的当前偏移量开始执行
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
    * 在进行任何处理之前必须调用此函数，并将填充以下字段：
   *  - currentBatchId 当前批次ID
   *  - committedOffsets 偏移量
   *  - availableOffsets 可偏移
   *  The basic structure of this method is as follows:
   *  这种方法的基本结构如下：
   *  Identify (from the offset log) the offsets used to run the last batch
   *  IF last batch exists THEN
    *  确定（从偏移日志）用于运行最后一批如果最后一批存在THEN的偏移量
   *    Set the next batch to be executed as the last recovered batch
   *    Check the commit log to see which batch was committed last
   *    IF the last batch was committed THEN
   *      Call getBatch using the last batch start and end offsets
   *      // ^^^^ above line is needed since some sources assume last batch always re-executes
   *      Setup for a new batch i.e., start = last batch end, and identify new end
   *    DONE
   *  ELSE
   *    Identify a brand new batch
   *  DONE
   */
  private def populateStartOffsets(sparkSessionToRunBatches: SparkSession): Unit = {
    offsetLog.getLatest() match {
      case Some((latestBatchId, nextOffsets)) =>
        /* First assume that we are re-executing the latest known batch
         * in the offset log
         * 首先假定我们正在重新执行偏移日志中的最新已知批处理*/
        currentBatchId = latestBatchId
        availableOffsets = nextOffsets.toStreamProgress(sources)
        /* Initialize committed offsets to a committed batch, which at this
         * is the second latest batch id in the offset log.
         * 将已提交的偏移初始化为已提交的批处理,这是偏移日志中的第二个最新批处理标识*/
        if (latestBatchId != 0) {
          val secondLatestBatchId = offsetLog.get(latestBatchId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestBatchId - 1} doesn't exist")
          }
          committedOffsets = secondLatestBatchId.toStreamProgress(sources)
        }

        // update offset metadata 更新偏移元数据
        nextOffsets.metadata.foreach { metadata =>
          OffsetSeqMetadata.setSessionConf(metadata, sparkSessionToRunBatches.conf)
          offsetSeqMetadata = OffsetSeqMetadata(
            metadata.batchWatermarkMs, metadata.batchTimestampMs, sparkSessionToRunBatches.conf)
        }

        /* identify the current batch id: if commit log indicates we successfully processed the
         * latest batch id in the offset log, then we can safely move to the next batch
         * i.e., committedBatchId + 1
         * 确定当前的批次ID：如果提交日志表明我们已经成功处理了偏移日志中的最新批次ID,
         * 那么我们可以安全地移动到下一批,即committedBatchId + 1*/
        batchCommitLog.getLatest() match {
          case Some((latestCommittedBatchId, _)) =>
            if (latestBatchId == latestCommittedBatchId) {
              /* The last batch was successfully committed, so we can safely process a
               * new next batch but first:
               * Make a call to getBatch using the offsets from previous batch.
               * because certain sources (e.g., KafkaSource) assume on restart the last
               * batch will be executed before getOffset is called again.
               * 最后一批已成功提交,所以我们可以安全地处理新的下一批,但首先：
               * 使用上一批的偏移量调用getBatch,
               * 因为某些源（例如KafkaSource）在重新启动时假定最后一批将在getOffset被再次调用之前执行。*/
              availableOffsets.foreach { ao: (Source, Offset) =>
                val (source, end) = ao
                if (committedOffsets.get(source).map(_ != end).getOrElse(true)) {
                  val start = committedOffsets.get(source)
                  source.getBatch(start, end)
                }
              }
              currentBatchId = latestCommittedBatchId + 1
              committedOffsets ++= availableOffsets
              // Construct a new batch be recomputing availableOffsets
              //构建一个新的批量重新计算availableOffsets
              constructNextBatch()
            } else if (latestCommittedBatchId < latestBatchId - 1) {
              logWarning(s"Batch completion log latest batch id is " +
                s"${latestCommittedBatchId}, which is not trailing " +
                s"batchid $latestBatchId by one")
            }
          case None => logInfo("no commit log present")
        }
        logDebug(s"Resuming at batch $currentBatchId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
      case None => // We are starting this stream for the first time.
        //我们是第一次开始这个流
        logInfo(s"Starting new streaming query.")
        currentBatchId = 0
        constructNextBatch()
    }
  }

  /**
   * Returns true if there is any new data available to be processed.
    * 如果有任何新的数据可以处理,则返回true
   */
  private def dataAvailable: Boolean = {
    availableOffsets.exists {
      case (source, available) =>
        committedOffsets
            .get(source)
            .map(committed => committed != available)
            .getOrElse(true)
    }
  }

  /**
   * Queries all of the sources to see if any new data is available. When there is new data the
   * batchId counter is incremented and a new log entry is written with the newest offsets.
    * 查询所有的来源,看看是否有新的数据可用,当有新数据时,batchId计数器递增,并用最新的偏移量写入新的日志条目
   */
  private def constructNextBatch(): Unit = {
    // Check to see what new data is available.
    //检查以查看可用的新数据
    val hasNewData = {
      awaitBatchLock.lock()
      try {
        val latestOffsets: Map[Source, Option[Offset]] = uniqueSources.map { s =>
          updateStatusMessage(s"Getting offsets from $s")
          reportTimeTaken("getOffset") {
            (s, s.getOffset)
          }
        }.toMap
        availableOffsets ++= latestOffsets.filter { case (s, o) => o.nonEmpty }.mapValues(_.get)

        if (dataAvailable) {
          true
        } else {
          noNewData = true
          false
        }
      } finally {
        awaitBatchLock.unlock()
      }
    }
    if (hasNewData) {
      var batchWatermarkMs = offsetSeqMetadata.batchWatermarkMs
      // Update the eventTime watermarks if we find any in the plan.
      //如果在计划中找到任何事件,请更新eventTime水印
      if (lastExecution != null) {
        lastExecution.executedPlan.collect {
          case e: EventTimeWatermarkExec => e
        }.zipWithIndex.foreach {
          case (e, index) if e.eventTimeStats.value.count > 0 =>
            logDebug(s"Observed event time stats $index: ${e.eventTimeStats.value}")
            val newWatermarkMs = e.eventTimeStats.value.max - e.delayMs
            val prevWatermarkMs = watermarkMsMap.get(index)
            if (prevWatermarkMs.isEmpty || newWatermarkMs > prevWatermarkMs.get) {
              watermarkMsMap.put(index, newWatermarkMs)
            }

          // Populate 0 if we haven't seen any data yet for this watermark node.
            //如果我们还没有看到这个水印节点的任何数据,填充0
          case (_, index) =>
            if (!watermarkMsMap.isDefinedAt(index)) {
              watermarkMsMap.put(index, 0)
            }
        }

        // Update the global watermark to the minimum of all watermark nodes.
        //将全局水印更新为所有水印节点的最小值
        // This is the safest option, because only the global watermark is fault-tolerant. Making
        // it the minimum of all individual watermarks guarantees it will never advance past where
        // any individual watermark operator would be if it were in a plan by itself.
        //这是最安全的选择,因为只有全局水印是容错的,使其成为所有单个水印的最小保证,
        //如果任何单个水印运算符本身在一个计划中，它将永远不会前进到哪里。
        if(!watermarkMsMap.isEmpty) {
          val newWatermarkMs = watermarkMsMap.minBy(_._2)._2
          if (newWatermarkMs > batchWatermarkMs) {
            logInfo(s"Updating eventTime watermark to: $newWatermarkMs ms")
            batchWatermarkMs = newWatermarkMs
          } else {
            logDebug(
              s"Event time didn't move: $newWatermarkMs < " +
                s"$batchWatermarkMs")
          }
        }
      }
      offsetSeqMetadata = offsetSeqMetadata.copy(
        batchWatermarkMs = batchWatermarkMs,
        //当前批处理时间戳,以毫秒为单位
        batchTimestampMs = triggerClock.getTimeMillis()) // Current batch timestamp in milliseconds

      updateStatusMessage("Writing offsets to log")
      reportTimeTaken("walCommit") {
        assert(offsetLog.add(
          currentBatchId,
          availableOffsets.toOffsetSeq(sources, offsetSeqMetadata)),
          s"Concurrent update to the log. Multiple streaming jobs detected for $currentBatchId")
        logInfo(s"Committed offsets for batch $currentBatchId. " +
          s"Metadata ${offsetSeqMetadata.toString}")

        // NOTE: The following code is correct because runBatches() processes exactly one
        // batch at a time. If we add pipeline parallelism (multiple batches in flight at
        // the same time), this cleanup logic will need to change.
        //注：下面的代码是正确的,因为runBatches（）一次只能处理一个批处理,
        // 如果我们添加管道并行性（同时在飞行中多个批次）,这个清理逻辑将需要改变
        // Now that we've updated the scheduler's persistent checkpoint, it is safe for the
        // sources to discard data from the previous batch.
        //现在我们已经更新了调度程序的持久性检查点,对于源放弃上一批的数据是安全的
        if (currentBatchId != 0) {
          val prevBatchOff = offsetLog.get(currentBatchId - 1)
          if (prevBatchOff.isDefined) {
            prevBatchOff.get.toStreamProgress(sources).foreach {
              case (src, off) => src.commit(off)
            }
          } else {
            throw new IllegalStateException(s"batch $currentBatchId doesn't exist")
          }
        }

        // It is now safe to discard the metadata beyond the minimum number to retain.
        // Note that purge is exclusive, i.e. it purges everything before the target ID.
        //现在放弃超过最小数量的元数据是安全的,请注意,清除是排他性的,即清除目标ID之前的所有内容。
        if (minBatchesToRetain < currentBatchId) {
          offsetLog.purge(currentBatchId - minBatchesToRetain)
          batchCommitLog.purge(currentBatchId - minBatchesToRetain)
        }
      }
    } else {
      awaitBatchLock.lock()
      try {
        // Wake up any threads that are waiting for the stream to progress.
        //唤醒正在等待流进展的线程
        awaitBatchLockCondition.signalAll()
      } finally {
        awaitBatchLock.unlock()
      }
    }
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
    * 处理`availableOffsets`和`committedOffsets`之间的任何数据
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runBatch(sparkSessionToRunBatch: SparkSession): Unit = {
    // Request unprocessed data from all sources.
    //请求来自所有来源的未经处理的数据
    newData = reportTimeTaken("getBatch") {
      availableOffsets.flatMap {
        case (source, available)
          if committedOffsets.get(source).map(_ != available).getOrElse(true) =>
          val current = committedOffsets.get(source)
          val batch = source.getBatch(current, available)
          assert(batch.isStreaming,
            s"DataFrame returned by getBatch from $source did not have isStreaming=true\n" +
              s"${batch.queryExecution.logical}")
          logDebug(s"Retrieving data from $source: $current -> $available")
          Some(source -> batch)
        case _ => None
      }
    }

    // A list of attributes that will need to be updated.
    //需要更新的属性列表
    val replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    //将逻辑计划中的来源替换为自上次批次以来已经到达的数据
    val withNewSources = logicalPlan transform {
      case StreamingExecutionRelation(source, output) =>
        newData.get(source).map { data =>
          val newPlan = data.logicalPlan
          assert(output.size == newPlan.output.size,
            s"Invalid batch: ${Utils.truncatedString(output, ",")} != " +
            s"${Utils.truncatedString(newPlan.output, ",")}")
          replacements ++= output.zip(newPlan.output)
          newPlan
        }.getOrElse {
          LocalRelation(output, isStreaming = true)
        }
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    //重新连接计划以使用源返回的新属性
    val replacementMap = AttributeMap(replacements)
    val triggerLogicalPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) =>
        replacementMap(a).withMetadata(a.metadata)
      case ct: CurrentTimestamp =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          ct.dataType)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(offsetSeqMetadata.batchTimestampMs,
          cd.dataType, cd.timeZoneId)
    }

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        triggerLogicalPlan,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)
      //强制执行计划的懒惰生成
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    val nextBatch =
      new Dataset(sparkSessionToRunBatch, lastExecution, RowEncoder(lastExecution.analyzed.schema))

    reportTimeTaken("addBatch") {
      SQLExecution.withNewExecutionId(sparkSessionToRunBatch, lastExecution) {
        sink.addBatch(currentBatchId, nextBatch)
      }
    }

    awaitBatchLock.lock()
    try {
      //唤醒正在等待流进展的线程。
      // Wake up any threads that are waiting for the stream to progress.
      awaitBatchLockCondition.signalAll()
    } finally {
      awaitBatchLock.unlock()
    }
  }

  override protected def postEvent(event: StreamingQueryListener.Event): Unit = {
    sparkSession.streams.postListenerEvent(event)
  }

  /** Stops all streaming sources safely.
    * 停止所有流媒体源安全 */
  private def stopSources(): Unit = {
    uniqueSources.foreach { source =>
      try {
        source.stop()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to stop streaming source: $source. Resources may have leaked.", e)
      }
    }
  }

  /**
   * Signals to the thread executing micro-batches that it should stop running after the next
   * batch. This method blocks until the thread stops running.
    * 指示执行微批处理的线程在下一批处理后应该停止运行,此方法阻塞,直到线程停止运行
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    //将状态设置为TERMINATED,以便批处理线程知道它是故意中断的
    state.set(TERMINATED)
    if (microBatchThread.isAlive) {
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
      microBatchThread.interrupt()
      microBatchThread.join()
      // microBatchThread may spawn new jobs, so we need to cancel again to prevent a leak
      //microBatchThread可能会产生新的工作,所以我们需要再次取消,以防止泄漏
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
    }
    logInfo(s"Query $prettyIdString was stopped")
  }

  /**
   * Blocks the current thread until processing for data from the given `source` has reached at
   * least the given `Offset`. This method is intended for use primarily when writing tests.
    * 阻塞当前线程,直到处理来自给定“源source”的数据已经达到至少给定的“偏移量 Offset”,
    * 此方法主要用于编写测试时使用
   */
  private[sql] def awaitOffset(source: Source, newOffset: Offset): Unit = {
    assertAwaitThread()
    def notDone = {
      val localCommittedOffsets = committedOffsets
      !localCommittedOffsets.contains(source) || localCommittedOffsets(source) != newOffset
    }

    while (notDone) {
      awaitBatchLock.lock()
      try {
        awaitBatchLockCondition.await(100, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
      } finally {
        awaitBatchLock.unlock()
      }
    }
    logDebug(s"Unblocked at $newOffset for $source")
  }

  /** A flag to indicate that a batch has completed with no new data available.
    * 一个标志,指示批次已经完成,没有新的可用数据 */
  @volatile private var noNewData = false

  /**
   * Assert that the await APIs should not be called in the stream thread. Otherwise, it may cause
   * dead-lock, e.g., calling any await APIs in `StreamingQueryListener.onQueryStarted` will block
   * the stream thread forever.
    * 断言不应该在流线程中调用await API,否则,可能会导致死锁,
    * 例如,在StreamingQueryListener.onQueryStarted中调用任何正在等待的API将永远阻塞流线程。
   */
  private def assertAwaitThread(): Unit = {
    if (microBatchThread eq Thread.currentThread) {
      throw new IllegalStateException(
        "Cannot wait for a query state from the same thread that is running the query")
    }
  }

  /**
   * Await until all fields of the query have been initialized.
    * 等待,直到查询的所有字段都被初始化
   */
  def awaitInitialization(timeoutMs: Long): Unit = {
    assertAwaitThread()
    require(timeoutMs > 0, "Timeout has to be positive")
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
    initializationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def processAllAvailable(): Unit = {
    assertAwaitThread()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
    awaitBatchLock.lock()
    try {
      noNewData = false
      while (true) {
        awaitBatchLockCondition.await(10000, TimeUnit.MILLISECONDS)
        if (streamDeathCause != null) {
          throw streamDeathCause
        }
        if (noNewData) {
          return
        }
      }
    } finally {
      awaitBatchLock.unlock()
    }
  }

  override def awaitTermination(): Unit = {
    assertAwaitThread()
    terminationLatch.await()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    assertAwaitThread()
    require(timeoutMs > 0, "Timeout has to be positive")
    terminationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    } else {
      !isActive
    }
  }

  /** Expose for tests  暴露测试*/
  def explainInternal(extended: Boolean): String = {
    if (lastExecution == null) {
      "No physical plan. Waiting for data."
    } else {
      val explain = StreamingExplainCommand(lastExecution, extended = extended)
      sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect()
        .map(_.getString(0)).mkString("\n")
    }
  }

  override def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    println(explainInternal(extended))
    // scalastyle:on println
  }

  override def explain(): Unit = explain(extended = false)

  override def toString: String = {
    s"Streaming Query $prettyIdString [state = $state]"
  }

  private def toDebugString(includeLogicalPlan: Boolean): String = {
    val debugString =
      s"""|=== Streaming Query ===
          |Identifier: $prettyIdString
          |Current Committed Offsets: $committedOffsets
          |Current Available Offsets: $availableOffsets
          |
          |Current State: $state
          |Thread State: ${microBatchThread.getState}""".stripMargin
    if (includeLogicalPlan) {
      debugString + s"\n\nLogical Plan:\n$logicalPlan"
    } else {
      debugString
    }
  }

  private def getBatchDescriptionString: String = {
    val batchDescription = if (currentBatchId < 0) "init" else currentBatchId.toString
    Option(name).map(_ + "<br/>").getOrElse("") +
      s"id = $id<br/>runId = $runId<br/>batch = $batchDescription"
  }
}

object StreamExecution {
  val QUERY_ID_KEY = "sql.streaming.queryId"
}

/**
 * A special thread to run the stream query. Some codes require to run in the StreamExecutionThread
 * and will use `classOf[StreamExecutionThread]` to check.
  * 运行流查询的特殊线程,有些代码需要在StreamExecutionThread中运行,并使用`classOf [StreamExecutionThread]`来检查
 */
abstract class StreamExecutionThread(name: String) extends UninterruptibleThread(name)
