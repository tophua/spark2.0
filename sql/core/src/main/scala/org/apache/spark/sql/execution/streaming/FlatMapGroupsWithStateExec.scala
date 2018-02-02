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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Expression, Literal, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.CompletionIterator

/**
 * Physical operator for executing `FlatMapGroupsWithState.`
 * 用于执行“FlatMapGroupsWithState.”的物理操作符
 * @param func function called on each group 函数调用每个组
 * @param keyDeserializer used to extract the key object for each group. 用于提取每个组的关键对象
 * @param valueDeserializer used to extract the items in the iterator from an input row.
  *                          用于从输入行中提取迭代器中的项目
 * @param groupingAttributes used to group the data 用于分组数据
 * @param dataAttributes used to read the data 用于读取数据
 * @param outputObjAttr used to define the output object 用于定义输出对象
 * @param stateEncoder used to serialize/deserialize state before calling `func`
  *                     用于在调用`func`之前序列化/反序列化状态
 * @param outputMode the output mode of `func` the output mode of `func`
 * @param timeoutConf used to timeout groups that have not received data in a while
  *                    用于超时在一段时间内没有收到数据的组
 * @param batchTimestampMs processing timestamp of the current batch.
  *                         处理当前批次的时间戳
 */
case class FlatMapGroupsWithStateExec(
    func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    stateEncoder: ExpressionEncoder[Any],
    outputMode: OutputMode,
    timeoutConf: GroupStateTimeout,
    batchTimestampMs: Option[Long],
    override val eventTimeWatermark: Option[Long],
    child: SparkPlan
  ) extends UnaryExecNode with ObjectProducerExec with StateStoreWriter with WatermarkSupport {

  import GroupStateImpl._

  private val isTimeoutEnabled = timeoutConf != NoTimeout
  private val timestampTimeoutAttribute =
    AttributeReference("timeoutTimestamp", dataType = IntegerType, nullable = false)()
  private val stateAttributes: Seq[Attribute] = {
    val encSchemaAttribs = stateEncoder.schema.toAttributes
    if (isTimeoutEnabled) encSchemaAttribs :+ timestampTimeoutAttribute else encSchemaAttribs
  }
  // Get the serializer for the state, taking into account whether we need to save timestamps
  //获取状态的序列化程序,同时考虑是否需要保存时间戳
  private val stateSerializer = {
    val encoderSerializer = stateEncoder.namedExpressions
    if (isTimeoutEnabled) {
      encoderSerializer :+ Literal(GroupStateImpl.NO_TIMESTAMP)
    } else {
      encoderSerializer
    }
  }
  // Get the deserializer for the state. Note that this must be done in the driver, as
  // resolving and binding of deserializer expressions to the encoded type can be safely done
  // only in the driver.
  //获取状态的解串器,请注意,这必须在驱动程序中完成,
  // 因为反序列化器表达式对编码类型的解析和绑定只能在驱动程序中安全地完成
  private val stateDeserializer = stateEncoder.resolveAndBind().deserializer


  /** Distribute by grouping attributes
    * 通过分组属性进行分配 */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  /** Ordering needed for using GroupingIterator
    * 使用GroupingIterator所需的顺序 */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    // Throw errors early if parameters are not as expected
    //如果参数不符合预期，则会提前发出错误
    timeoutConf match {
      case ProcessingTimeTimeout =>
        require(batchTimestampMs.nonEmpty)
      case EventTimeTimeout =>
        require(eventTimeWatermark.nonEmpty)  // watermark value has been populated
        require(watermarkExpression.nonEmpty) // input schema has watermark attribute
      case _ =>
    }

    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateInfo,
      groupingAttributes.toStructType,
      stateAttributes.toStructType,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val updater = new StateStoreUpdater(store)

        // If timeout is based on event time, then filter late data based on watermark
      //如果超时是基于事件时间,则过滤基于水印的数据
        val filteredIter = watermarkPredicateForData match {
          case Some(predicate) if timeoutConf == EventTimeTimeout =>
            iter.filter(row => !predicate.eval(row))
          case _ =>
            iter
        }

        // Generate a iterator that returns the rows grouped by the grouping function
        // Note that this code ensures that the filtering for timeout occurs only after
        // all the data has been processed. This is to ensure that the timeout information of all
        // the keys with data is updated before they are processed for timeouts.
      //生成一个迭代器，用于返回按分组函数分组的行,请注意,此代码确保只有在处理了所有数据之后才会对超时进行筛选。
        // 这是为了确保在处理超时之前,更新带有数据的所有密钥的超时信息。
        val outputIterator =
          updater.updateStateForKeysWithData(filteredIter) ++ updater.updateStateForTimedOutKeys()

        // Return an iterator of all the rows generated by all the keys, such that when fully
        // consumed, all the state updates will be committed by the state store
      //返回由所有键生成的所有行的迭代器,这样当完全使用时,所有状态更新将由状态存储提交
        CompletionIterator[InternalRow, Iterator[InternalRow]](
          outputIterator,
          {
            store.commit()
            setStoreMetrics(store)
          }
        )
    }
  }

  /** Helper class to update the state store
    * 助手类来更新状态存储 */
  class StateStoreUpdater(store: StateStore) {

    // Converters for translating input keys, values, output data between rows and Java objects
    //转换器用于翻译输入keys,values,在行和Java对象之间输出数据
    private val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
    private val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
    private val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

    // Converters for translating state between rows and Java objects
    //用于在行和Java对象之间转换状态的转换器
    private val getStateObjFromRow = ObjectOperator.deserializeRowToObject(
      stateDeserializer, stateAttributes)
    private val getStateRowFromObj = ObjectOperator.serializeObjectToRow(stateSerializer)

    // Index of the additional metadata fields in the state row
    //状态行中的附加元数据字段的索引
    private val timeoutTimestampIndex = stateAttributes.indexOf(timestampTimeoutAttribute)

    // Metrics
    private val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    private val numOutputRows = longMetric("numOutputRows")

    /**
     * For every group, get the key, values and corresponding state and call the function,
     * and return an iterator of rows
      * 对于每个组,获取键,值和相应的状态并调用函数,并返回行的迭代器
     */
    def updateStateForKeysWithData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
      val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
      groupedIter.flatMap { case (keyRow, valueRowIter) =>
        val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
        callFunctionAndUpdateState(
          keyUnsafeRow,
          valueRowIter,
          store.get(keyUnsafeRow),
          hasTimedOut = false)
      }
    }

    /** Find the groups that have timeout set and are timing out right now, and call the function
      * 找到有超时设置的组,并立即超时,并调用该函数 */
    def updateStateForTimedOutKeys(): Iterator[InternalRow] = {
      if (isTimeoutEnabled) {
        val timeoutThreshold = timeoutConf match {
          case ProcessingTimeTimeout => batchTimestampMs.get
          case EventTimeTimeout => eventTimeWatermark.get
          case _ =>
            throw new IllegalStateException(
              s"Cannot filter timed out keys for $timeoutConf")
        }
        val timingOutKeys = store.getRange(None, None).filter { rowPair =>
          val timeoutTimestamp = getTimeoutTimestamp(rowPair.value)
          timeoutTimestamp != NO_TIMESTAMP && timeoutTimestamp < timeoutThreshold
        }
        timingOutKeys.flatMap { rowPair =>
          callFunctionAndUpdateState(rowPair.key, Iterator.empty, rowPair.value, hasTimedOut = true)
        }
      } else Iterator.empty
    }

    /**
     * Call the user function on a key's data, update the state store, and return the return data
     * iterator. Note that the store updating is lazy, that is, the store will be updated only
     * after the returned iterator is fully consumed.
     *
      * 调用关键数据的用户函数,更新状态存储,并返回返回数据迭代器,
      * 请注意,存储更新是懒惰的,也就是说只有在返回的迭代器被完全使用之后,存储才会被更新。
      *
     * @param keyRow Row representing the key, cannot be null 表示键的行不能为空
     * @param valueRowIter Iterator of values as rows, cannot be null, but can be empty
      *                     值为行的迭代器,不能为空,但可以为空
     * @param prevStateRow Row representing the previous state, can be null
      *                     表示前一个状态的行可以为null
     * @param hasTimedOut Whether this function is being called for a key timeout
      *                    这个函数是否被调用了一个按键超时
     */
    private def callFunctionAndUpdateState(
        keyRow: UnsafeRow,
        valueRowIter: Iterator[InternalRow],
        prevStateRow: UnsafeRow,
        hasTimedOut: Boolean): Iterator[InternalRow] = {

      val keyObj = getKeyObj(keyRow)  // convert key to objects 将键转换为对象
      val valueObjIter = valueRowIter.map(getValueObj.apply) // convert value rows to objects 将值行转换为对象
      val stateObj = getStateObj(prevStateRow)
      val keyedState = GroupStateImpl.createForStreaming(
        Option(stateObj),
        batchTimestampMs.getOrElse(NO_TIMESTAMP),
        eventTimeWatermark.getOrElse(NO_TIMESTAMP),
        timeoutConf,
        hasTimedOut)

      // Call function, get the returned objects and convert them to rows
      //调用函数,获取返回的对象并将其转换为行
      val mappedIterator = func(keyObj, valueObjIter, keyedState).map { obj =>
        numOutputRows += 1
        getOutputRow(obj)
      }

      // When the iterator is consumed, then write changes to state
      //当迭代器被使用时,然后写入更改状态
      def onIteratorCompletion: Unit = {

        val currentTimeoutTimestamp = keyedState.getTimeoutTimestamp
        // If the state has not yet been set but timeout has been set, then
        // we have to generate a row to save the timeout. However, attempting serialize
        // null using case class encoder throws -
        //如果状态尚未设置,但超时已设置,那么我们必须生成一行来保存超时,但是,尝试使用大小写编码器序列化null会抛出 -
        //    java.lang.NullPointerException: Null value appeared in non-nullable field:
        //    If the schema is inferred from a Scala tuple / case class, or a Java bean, please
        //    try to use scala.Option[_] or other nullable types.
        if (!keyedState.exists && currentTimeoutTimestamp != NO_TIMESTAMP) {
          throw new IllegalStateException(
            "Cannot set timeout when state is not defined, that is, state has not been" +
              "initialized or has been removed")
        }

        if (keyedState.hasRemoved) {
          store.remove(keyRow)
          numUpdatedStateRows += 1

        } else {
          val previousTimeoutTimestamp = getTimeoutTimestamp(prevStateRow)
          val stateRowToWrite = if (keyedState.hasUpdated) {
            getStateRow(keyedState.get)
          } else {
            prevStateRow
          }

          val hasTimeoutChanged = currentTimeoutTimestamp != previousTimeoutTimestamp
          val shouldWriteState = keyedState.hasUpdated || hasTimeoutChanged

          if (shouldWriteState) {
            if (stateRowToWrite == null) {
              // This should never happen because checks in GroupStateImpl should avoid cases
              // where empty state would need to be written
              throw new IllegalStateException("Attempting to write empty state")
            }
            setTimeoutTimestamp(stateRowToWrite, currentTimeoutTimestamp)
            store.put(keyRow, stateRowToWrite)
            numUpdatedStateRows += 1
          }
        }
      }

      // Return an iterator of rows such that fully consumed, the updated state value will be saved
      //返回完全消耗的行的迭代器,更新的状态值将被保存
      CompletionIterator[InternalRow, Iterator[InternalRow]](mappedIterator, onIteratorCompletion)
    }

    /** Returns the state as Java object if defined
      * 如果已定义,则返回Java对象的状态*/
    def getStateObj(stateRow: UnsafeRow): Any = {
      if (stateRow != null) getStateObjFromRow(stateRow) else null
    }

    /** Returns the row for an updated state
      * 返回更新状态的行*/
    def getStateRow(obj: Any): UnsafeRow = {
      assert(obj != null)
      getStateRowFromObj(obj)
    }

    /** Returns the timeout timestamp of a state row is set
      * 返回设置状态行的超时时间戳*/
    def getTimeoutTimestamp(stateRow: UnsafeRow): Long = {
      if (isTimeoutEnabled && stateRow != null) {
        stateRow.getLong(timeoutTimestampIndex)
      } else NO_TIMESTAMP
    }

    /** Set the timestamp in a state row
      * 在状态行中设置时间戳*/
    def setTimeoutTimestamp(stateRow: UnsafeRow, timeoutTimestamps: Long): Unit = {
      if (isTimeoutEnabled) stateRow.setLong(timeoutTimestampIndex, timeoutTimestamps)
    }
  }
}
