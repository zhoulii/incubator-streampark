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
package org.apache.streampark.flink.shims.context

import com.esotericsoftware.kryo.Serializer
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.io.{FileInputFormat, FilePathFilter, InputFormat}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.{JobExecutionResult, RuntimeExecutionMode}
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions._
import org.apache.flink.table.module.{Module, ModuleEntry}
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row
import org.apache.streampark.common.conf.ConfigConst._
import org.apache.streampark.flink.shims.base.EnhancerImplicit.EnhanceParameterTool
import org.apache.streampark.flink.shims.base.FlinkSqlExecutor

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.{Optional, List => JavaList}

/**
 * Integration api of stream and table
 */
abstract class StreamTableContext(
                                   val parameter: ParameterTool,
                                   private val streamEnv: StreamExecutionEnvironment,
                                   private val tableEnv: StreamTableEnvironment) extends TableContextTrait {

  /**
   * Once a Table has been converted to a DataStream, the DataStream job must be executed
   * using the execute method of the StreamExecutionEnvironment.
   */
  var isConvertedToDataStream: Boolean = false

  /**
   * Recommended to use this Api to start tasks
   */
  def start(name: String = null): JobExecutionResult = {
    val appName = parameter.getAppName(name, true)
    execute(appName)
  }

  def execute(jobName: String): JobExecutionResult = {
    printLogo(s"FlinkStreamTable $jobName Starting...")
    if (isConvertedToDataStream) {
      streamEnv.execute(jobName)
    } else null
  }

  def sql(sql: String = null)(implicit callback: String => Unit = null): Unit = {
    FlinkSqlExecutor.executeSql(sql, parameter, this)
  }

  // ------------------------------------------------------------------
  // ...streamEnv api start...
  // ------------------------------------------------------------------

  def $getCachedFiles: JavaList[tuple.Tuple2[String, DistributedCache.DistributedCacheEntry]] = this.streamEnv.getCachedFiles

  def $getJobListeners: JavaList[JobListener] = this.streamEnv.getJobListeners

  def $setParallelism(parallelism: Int): Unit = this.streamEnv.setParallelism(parallelism)

  def $setRuntimeMode(executionMode: RuntimeExecutionMode): StreamExecutionEnvironment =
    this.streamEnv.setRuntimeMode(executionMode)

  def $setMaxParallelism(maxParallelism: Int): Unit = this.streamEnv.setMaxParallelism(maxParallelism)

  def $getParallelism: Int = this.streamEnv.getParallelism

  def $getMaxParallelism: Int = this.streamEnv.getMaxParallelism

  def $setBufferTimeout(timeoutMillis: Long): StreamExecutionEnvironment = this.streamEnv.setBufferTimeout(timeoutMillis)

  def $getBufferTimeout: Long = this.streamEnv.getBufferTimeout

  def $disableOperatorChaining(): StreamExecutionEnvironment = this.streamEnv.disableOperatorChaining()

  def $getCheckpointConfig: CheckpointConfig = this.streamEnv.getCheckpointConfig

  def $enableCheckpointing(interval: Long, mode: CheckpointingMode): StreamExecutionEnvironment =
    this.streamEnv.enableCheckpointing(interval, mode)

  def $enableCheckpointing(interval: Long): StreamExecutionEnvironment = this.streamEnv.enableCheckpointing(interval)

  def $getCheckpointingMode: CheckpointingMode = this.streamEnv.getCheckpointingMode

  def $setStateBackend(backend: StateBackend): StreamExecutionEnvironment = this.streamEnv.setStateBackend(backend)

  def $getStateBackend: StateBackend = this.streamEnv.getStateBackend

  def $setRestartStrategy(restartStrategyConfiguration: RestartStrategies.RestartStrategyConfiguration): Unit =
    this.streamEnv.setRestartStrategy(restartStrategyConfiguration)

  def $getRestartStrategy: RestartStrategies.RestartStrategyConfiguration = this.streamEnv.getRestartStrategy

  def $setNumberOfExecutionRetries(numRetries: Int): Unit = this.streamEnv.setNumberOfExecutionRetries(numRetries)

  def $getNumberOfExecutionRetries: Int = this.streamEnv.getNumberOfExecutionRetries

  def $addDefaultKryoSerializer[T <: Serializer[_] with Serializable](`type`: Class[_], serializer: T): Unit =
    this.streamEnv.addDefaultKryoSerializer(`type`, serializer)

  def $addDefaultKryoSerializer(`type`: Class[_], serializerClass: Class[_ <: Serializer[_]]): Unit =
    this.streamEnv.addDefaultKryoSerializer(`type`, serializerClass)

  def $registerTypeWithKryoSerializer[T <: Serializer[_] with Serializable](clazz: Class[_], serializer: T): Unit =
    this.streamEnv.registerTypeWithKryoSerializer(clazz, serializer)

  def $registerTypeWithKryoSerializer(clazz: Class[_], serializer: Class[_ <: Serializer[_]]): Unit =
    this.streamEnv.registerTypeWithKryoSerializer(clazz, serializer)

  def $registerType(typeClass: Class[_]): Unit = this.streamEnv.registerType(typeClass)

  def $getStreamTimeCharacteristic: TimeCharacteristic = this.streamEnv.getStreamTimeCharacteristic

  def $configure(configuration: ReadableConfig, classLoader: ClassLoader): Unit =
    this.streamEnv.configure(configuration, classLoader)

  def $fromSequence(from: Long, to: Long): DataStream[JLong] = this.streamEnv.fromSequence(from, to)

  def $fromElements[T](data: T*)(implicit info: TypeInformation[T]): DataStream[T] = this.streamEnv.fromElements(data: _*)


  def $readTextFile(filePath: String): DataStream[String] = this.streamEnv.readTextFile(filePath)

  def $readTextFile(filePath: String, charsetName: String): DataStream[String] =
    this.streamEnv.readTextFile(filePath, charsetName)

  def $readFile[T](inputFormat: FileInputFormat[T], filePath: String)(implicit info: TypeInformation[T]): DataStream[T] =
    this.streamEnv.readFile(inputFormat, filePath)

  def $readFile[T](inputFormat: FileInputFormat[T],
                   filePath: String,
                   watchType: FileProcessingMode,
                   interval: Long)(implicit info: TypeInformation[T]): DataStream[T] =
    this.streamEnv.readFile(inputFormat, filePath, watchType, interval)

  def $socketTextStream(hostname: String, port: Int, delimiter: Char, maxRetry: Long): DataStream[String] =
    this.streamEnv.socketTextStream(hostname, port, delimiter, maxRetry)

  def $createInput[T](inputFormat: InputFormat[T, _])(implicit info: TypeInformation[T]): DataStream[T] =
    this.streamEnv.createInput(inputFormat)

  def $addSource[T](function: SourceFunction[T])(implicit info: TypeInformation[T]): DataStream[T] =
    this.streamEnv.addSource(function)


  def $fromSource[T](source: Source[T, _ <: SourceSplit, _],
                     watermarkStrategy: WatermarkStrategy[T],
                     sourceName: String)
                    (implicit info: TypeInformation[T]): DataStream[T] =
    this.streamEnv.fromSource(source, watermarkStrategy, sourceName)

  def $registerJobListener(jobListener: JobListener): Unit = this.streamEnv.registerJobListener(jobListener)

  def $clearJobListeners(): Unit = this.streamEnv.clearJobListeners()

  def $executeAsync(): JobClient = this.streamEnv.executeAsync()

  def $executeAsync(jobName: String): JobClient = this.streamEnv.executeAsync(jobName)

  def $getExecutionPlan: String = this.streamEnv.getExecutionPlan

  def $getStreamGraph: StreamGraph = this.streamEnv.getStreamGraph

  def $registerCachedFile(filePath: String, name: String): Unit = this.streamEnv.registerCachedFile(filePath, name)

  def $registerCachedFile(filePath: String, name: String, executable: Boolean): Unit =
    this.streamEnv.registerCachedFile(filePath, name, executable)

  def $isUnalignedCheckpointsEnabled: Boolean = this.streamEnv.isUnalignedCheckpointsEnabled

  def $isForceUnalignedCheckpoints: Boolean = this.streamEnv.isForceUnalignedCheckpoints

  def $enableCheckpointing(interval: Long, mode: CheckpointingMode, force: Boolean): StreamExecutionEnvironment =
    this.streamEnv.enableCheckpointing(interval, mode, force)

  def $enableCheckpointing(): StreamExecutionEnvironment = this.streamEnv.enableCheckpointing()

  def $generateSequence(from: Long, to: Long): DataStream[JLong] = this.streamEnv.generateSequence(from, to)

  def $readFileStream(StreamPath: String,
                      intervalMillis: Long,
                      watchType: FileMonitoringFunction.WatchType): DataStream[String] =
    this.streamEnv.readFileStream(StreamPath, intervalMillis, watchType)

  def $readFile[T](
                    inputFormat: FileInputFormat[T],
                    filePath: String,
                    watchType: FileProcessingMode,
                    interval: Long,
                    filter: FilePathFilter)(implicit info: TypeInformation[T]): DataStream[T] =
    this.streamEnv.readFile(inputFormat, filePath, watchType, interval, filter)

  // ------------------------------------------------------------------
  // ...tableEnv api start...
  // ------------------------------------------------------------------

  def fromDataStream[T](dataStream: DataStream[T]): Table = tableEnv.fromDataStream(dataStream)

  def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table = tableEnv.fromDataStream(dataStream, fields: _*)

  def createTemporaryView[T](path: String, dataStream: DataStream[T]): Unit = tableEnv.createTemporaryView(path, dataStream)

  def createTemporaryView[T](path: String, dataStream: DataStream[T], fields: Expression*): Unit =
    tableEnv.createTemporaryView(path, dataStream, fields: _*)

  def fromValues(values: Expression*): Table = tableEnv.fromValues(values)

  def fromValues(rowType: AbstractDataType[_], values: Expression*): Table = tableEnv.fromValues(rowType, values: _*)

  def fromValues(values: java.lang.Iterable[_]): Table = tableEnv.fromValues(values)

  def fromValues(rowType: AbstractDataType[_], values: java.lang.Iterable[_]): Table = tableEnv.fromValues(rowType, values)

  def toRetractStream[T](table: Table)(implicit info: TypeInformation[T]): DataStream[tuple.Tuple2[JBoolean, T]] = {
    isConvertedToDataStream = true
    tableEnv.toRetractStream(table, info)
  }

  def toAppendStream[T](table: Table)(implicit info: TypeInformation[T]): DataStream[T] = {
    isConvertedToDataStream = true
    tableEnv.toAppendStream(table, info)
  }

  def registerCatalog(catalogName: String, catalog: Catalog): Unit = tableEnv.registerCatalog(catalogName, catalog)

  def getCatalog(catalogName: String): Optional[Catalog] = tableEnv.getCatalog(catalogName)

  def loadModule(moduleName: String, module: Module): Unit = tableEnv.loadModule(moduleName, module)

  def unloadModule(moduleName: String): Unit = tableEnv.unloadModule(moduleName)

  def createTemporarySystemFunction(name: String, functionClass: Class[_ <: UserDefinedFunction]): Unit =
    tableEnv.createTemporarySystemFunction(name, functionClass)

  def createTemporarySystemFunction(name: String, functionInstance: UserDefinedFunction): Unit =
    tableEnv.createTemporarySystemFunction(name, functionInstance)

  def dropTemporarySystemFunction(name: String): Boolean = tableEnv.dropTemporarySystemFunction(name)

  def createFunction(path: String, functionClass: Class[_ <: UserDefinedFunction]): Unit = tableEnv.createFunction(path, functionClass)

  def createFunction(path: String, functionClass: Class[_ <: UserDefinedFunction], ignoreIfExists: Boolean): Unit =
    tableEnv.createFunction(path, functionClass)

  def dropFunction(path: String): Boolean = tableEnv.dropFunction(path)

  def createTemporaryFunction(path: String, functionClass: Class[_ <: UserDefinedFunction]): Unit =
    tableEnv.createTemporaryFunction(path, functionClass)

  def createTemporaryFunction(path: String, functionInstance: UserDefinedFunction): Unit = tableEnv.createTemporaryFunction(path, functionInstance)

  def dropTemporaryFunction(path: String): Boolean = tableEnv.dropTemporaryFunction(path)

  def createTemporaryView(path: String, view: Table): Unit = tableEnv.createTemporaryView(path, view)

  def from(path: String): Table = tableEnv.from(path)

  def listCatalogs(): Array[String] = tableEnv.listCatalogs()

  def listModules(): Array[String] = tableEnv.listModules()

  def listDatabases(): Array[String] = tableEnv.listDatabases()

  def listTables(): Array[String] = tableEnv.listTables()

  def listViews(): Array[String] = tableEnv.listViews()

  def listTemporaryTables(): Array[String] = tableEnv.listTemporaryTables

  def listTemporaryViews(): Array[String] = tableEnv.listTemporaryViews()

  def listUserDefinedFunctions(): Array[String] = tableEnv.listUserDefinedFunctions()

  def listFunctions(): Array[String] = tableEnv.listFunctions()

  def dropTemporaryTable(path: String): Boolean = tableEnv.dropTemporaryTable(path)

  def dropTemporaryView(path: String): Boolean = tableEnv.dropTemporaryView(path)

  def explainSql(statement: String, extraDetails: ExplainDetail*): String = tableEnv.explainSql(statement, extraDetails: _*)

  def sqlQuery(query: String): Table = tableEnv.sqlQuery(query)

  def executeSql(statement: String): TableResult = tableEnv.executeSql(statement)

  def createStatementSet(): StatementSet = tableEnv.createStatementSet()

  def getCurrentCatalog: String = tableEnv.getCurrentCatalog

  def useCatalog(catalogName: String): Unit = tableEnv.useCatalog(catalogName)

  def getCurrentDatabase: String = tableEnv.getCurrentDatabase

  def useDatabase(databaseName: String): Unit = tableEnv.useDatabase(databaseName)

  def getConfig: TableConfig = tableEnv.getConfig

  def registerFunction[T](name: String, tf: TableFunction[T])(implicit info: TypeInformation[T]): Unit =
    tableEnv.registerFunction(name, tf)

  def registerFunction[T, ACC](name: String, f: AggregateFunction[T, ACC])(implicit
                                                                           info1: TypeInformation[T],
                                                                           info2: TypeInformation[ACC]): Unit = tableEnv.registerFunction(name, f)

  def registerFunction[T, ACC](name: String, f: TableAggregateFunction[T, ACC])(implicit
                                                                                info1: TypeInformation[T],
                                                                                info2: TypeInformation[ACC]): Unit = tableEnv.registerFunction(name, f)

  def registerDataStream[T](name: String, dataStream: DataStream[T]): Unit = tableEnv.registerDataStream(name, dataStream)

  def registerFunction(name: String, function: ScalarFunction): Unit = tableEnv.registerFunction(name, function)

  def registerTable(name: String, table: Table): Unit = tableEnv.registerTable(name, table)

  def scan(tablePath: String*): Table = tableEnv.scan(tablePath: _*)

  def getCompletionHints(statement: String, position: Int): Array[String] = tableEnv.getCompletionHints(statement, position)

  // -------------------------------------------------------
  // these apis are newly added in Flink 1.13
  // -------------------------------------------------------

  def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table

  def fromChangelogStream(dataStream: DataStream[Row]): Table

  def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table

  def fromChangelogStream(dataStream: DataStream[Row], schema: Schema, changelogMode: ChangelogMode): Table

  def createTemporaryView[T](path: String, dataStream: DataStream[T], schema: Schema): Unit

  def toDataStream(table: Table): DataStream[Row]

  def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T]

  def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): DataStream[T]

  def toChangelogStream(table: Table): DataStream[Row]

  def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row]

  def toChangelogStream(table: Table, targetSchema: Schema, changelogMode: ChangelogMode): DataStream[Row]

  def useModules(strings: String*): Unit

  def listFullModules(): Array[ModuleEntry]

  // -------------------------------------------------------
  // these apis were already deleted in Flink 1.14
  // -------------------------------------------------------

  def $getStreamGraph(jobName: String): StreamGraph

  def $getStreamGraph(jobName: String, clearTransformations: JBoolean): StreamGraph

  // -------------------------------------------------------
  // these apis are newly added in Flink 1.14
  // -------------------------------------------------------

  def createTable(path: String, descriptor: TableDescriptor): Unit

  def createTemporaryTable(path: String, descriptor: TableDescriptor): Unit

  def $getStreamGraph(clearTransformations: Boolean): StreamGraph

  def from(descriptor: TableDescriptor): org.apache.flink.table.api.Table

  // -------------------------------------------------------
  // these apis were already deleted in Flink 1.15
  // -------------------------------------------------------

  def sqlUpdate(stmt: String): Unit

  def insertInto(targetPath: String, table: Table): Unit

  def explain(table: Table): String

  def explain(table: Table, extended: JBoolean): String

  def explain(extended: JBoolean): String

  def fromTableSource(source: TableSource[_]): Table

  // -------------------------------------------------------
  // these apis are newly added in Flink 1.15
  // -------------------------------------------------------

  def listTables(s: String, s1: String): Array[String]

  def loadPlan(planReference: PlanReference): CompiledPlan

  def compilePlanSql(s: String): CompiledPlan

  def getStreamTableEnvClass: Class[_] = {
    try {
      Class.forName("org.apache.flink.table.api.bridge.java.StreamTableEnvironment")
    } catch {
      case e: ClassNotFoundException =>
        throw new RuntimeException("Failed to find class StreamTableEnvironment", e)
    }
  }

  def getStreamEnvClass: Class[_] = try {
    Class.forName("org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
  } catch {
    case e: ClassNotFoundException =>
      throw new RuntimeException("Failed to find class StreamExecutionEnvironment", e)
  }
}
