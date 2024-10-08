/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.factories.utils

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.legacy.table.sinks.{AppendStreamTableSink, StreamTableSink}
import org.apache.flink.legacy.table.sources.StreamTableSource
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR
import org.apache.flink.table.functions.{AsyncTableFunction, TableFunction}
import org.apache.flink.table.legacy.api.TableSchema
import org.apache.flink.table.legacy.factories.{TableSinkFactory, TableSourceFactory}
import org.apache.flink.table.legacy.sinks.TableSink
import org.apache.flink.table.legacy.sources.LookupableTableSource
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory.{getCollectionSink, getCollectionSource}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.TableSchemaUtils.getPhysicalSchema
import org.apache.flink.types.Row

import java.io.IOException
import java.util
import java.util.{ArrayList => JArrayList, LinkedList => JLinkedList, List => JList, Map => JMap}

import scala.collection.JavaConversions._

class TestCollectionTableFactory extends TableSourceFactory[Row] with TableSinkFactory[Row] {

  override def createTableSource(context: TableSourceFactory.Context): StreamTableSource[Row] = {
    getCollectionSource(context)
  }

  override def createTableSink(context: TableSinkFactory.Context): StreamTableSink[Row] = {
    getCollectionSink(context)
  }

  override def requiredContext(): JMap[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR, "COLLECTION")
    context
  }

  override def supportedProperties(): JList[String] = {
    val supported = new JArrayList[String]()
    supported.add("*")
    supported
  }
}

object TestCollectionTableFactory {
  val IS_BOUNDED = "is-bounded"

  val SOURCE_DATA = new JLinkedList[Row]()
  val DIM_DATA = new JLinkedList[Row]()
  val RESULT = new JLinkedList[Row]()
  private var emitIntervalMS = -1L

  def initData(sourceData: JList[Row]): Unit = {
    initData(sourceData, List(), -1L)
  }

  def initData(
      sourceData: JList[Row],
      dimData: JList[Row] = List(),
      emitInterval: Long = -1L): Unit = {
    SOURCE_DATA.addAll(sourceData)
    DIM_DATA.addAll(dimData)
    emitIntervalMS = emitInterval
  }

  def reset(): Unit = {
    RESULT.clear()
    SOURCE_DATA.clear()
    DIM_DATA.clear()
    emitIntervalMS = -1L
  }

  def getResult: util.List[Row] = RESULT

  def getCollectionSource(context: TableSourceFactory.Context): CollectionTableSource = {
    val schema = context.getTable.getSchema
    val isBounded = context.getTable.getOptions.getOrDefault(IS_BOUNDED, "true").toBoolean
    new CollectionTableSource(emitIntervalMS, getPhysicalSchema(schema), isBounded)
  }

  def getCollectionSink(context: TableSinkFactory.Context): CollectionTableSink = {
    val schema = context.getTable.getSchema
    new CollectionTableSink(getPhysicalSchema(schema))
  }

  /** Table source of collection. */
  class CollectionTableSource(
      val emitIntervalMs: Long,
      val schema: TableSchema,
      val bounded: Boolean)
    extends StreamTableSource[Row]
    with LookupableTableSource[Row] {

    private val dataType = schema.toRowDataType
    private val typeInfo = fromDataTypeToTypeInfo(dataType).asInstanceOf[TypeInformation[Row]]

    override def isBounded: Boolean = bounded

    override def getDataStream(streamEnv: StreamExecutionEnvironment): DataStreamSource[Row] = {
      streamEnv.createInput(
        new TestCollectionInputFormat[Row](
          emitIntervalMs,
          SOURCE_DATA,
          typeInfo.createSerializer(new SerializerConfigImpl)),
        typeInfo)
    }

    override def getProducedDataType: DataType = dataType

    override def getTableSchema: TableSchema = {
      schema
    }

    override def getLookupFunction(lookupKeys: Array[String]): TemporalTableFetcher = {
      new TemporalTableFetcher(DIM_DATA, lookupKeys.map(schema.getFieldNames.indexOf(_)))
    }

    override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[Row] = null

    override def isAsyncEnabled: Boolean = false
  }

  /** Table sink of collection. */
  class CollectionTableSink(val schema: TableSchema) extends AppendStreamTableSink[Row] {

    override def getConsumedDataType: DataType = schema.toRowDataType

    override def getTableSchema: TableSchema = schema

    override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
      val dataType = schema.toRowDataType
      val typeInfo = fromDataTypeToTypeInfo(dataType).asInstanceOf[TypeInformation[Row]]
      dataStream.addSink(new UnsafeMemorySinkFunction(typeInfo)).setParallelism(1)
    }

    override def configure(
        fieldNames: Array[String],
        fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = this
  }

  /** Sink function of unsafe memory. */
  class UnsafeMemorySinkFunction(outputType: TypeInformation[Row]) extends RichSinkFunction[Row] {
    private var serializer: TypeSerializer[Row] = _

    override def open(openContext: OpenContext): Unit = {
      serializer = outputType.createSerializer(new SerializerConfigImpl)
    }

    @throws[Exception]
    override def invoke(row: Row): Unit = {
      RESULT.add(serializer.copy(row))
    }
  }

  /** Collection inputFormat for testing. */
  class TestCollectionInputFormat[T](
      val emitIntervalMs: Long,
      val dataSet: java.util.Collection[T],
      val serializer: TypeSerializer[T])
    extends CollectionInputFormat[T](dataSet, serializer) {
    @throws[IOException]
    override def reachedEnd: Boolean = {
      if (emitIntervalMs > 0) {
        try
          Thread.sleep(emitIntervalMs)
        catch {
          case _: InterruptedException =>
        }
      }
      super.reachedEnd
    }
  }

  /** Dimension table source fetcher. */
  class TemporalTableFetcher(val dimData: JLinkedList[Row], val keys: Array[Int])
    extends TableFunction[Row] {

    @throws[Exception]
    def eval(values: Any*): Unit = {
      for (data <- dimData) {
        var matched = true
        var idx = 0
        while (matched && idx < keys.length) {
          val dimField = data.getField(keys(idx))
          val inputField = values(idx)
          matched = dimField.equals(inputField)
          idx += 1
        }
        if (matched) {
          // copy the row data
          val ret = new Row(data.getArity)
          (0 until data.getArity).foreach(idx => ret.setField(idx, data.getField(idx)))
          collect(ret)
        }
      }
    }
  }
}
