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

import org.apache.flink.FlinkVersion
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{CompiledPlan, PlanReference, Schema, Table, TableDescriptor}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row
import org.apache.streampark.flink.shims.base.{FlinkTableInitializer, StreamTableEnvConfig}

import java.lang.{Boolean => JBoolean}

class StreamTableContextV112(override val parameter: ParameterTool, private val streamEnv: StreamExecutionEnvironment, private val tableEnv: StreamTableEnvironment)
  extends StreamTableContext(parameter, streamEnv, tableEnv) {

  def this(args: (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment)) = this(args._1, args._2, args._3)

  def this(args: StreamTableEnvConfig) = this(FlinkTableInitializer.initialize(args))

  override def $getStreamGraph(jobName: String): StreamGraph = {
    val getStreamGraph = getStreamTableEnvClass.getMethod("getStreamGraph", classOf[String])
    getStreamGraph.invoke(streamEnv, jobName).asInstanceOf[StreamGraph]
  }

  override def $getStreamGraph(jobName: String, clearTransformations: JBoolean): StreamGraph = {
    val getStreamGraph = getStreamTableEnvClass.getMethod("getStreamGraph", classOf[String], classOf[JBoolean])
    getStreamGraph.invoke(streamEnv, jobName, clearTransformations).asInstanceOf[StreamGraph]
  }

  override def insertInto(targetPath: String, table: Table): Unit = {
    val insertInto = getStreamTableEnvClass.getMethod("insertInto", classOf[String], classOf[Table])
    insertInto.invoke(tableEnv, targetPath, table)
  }

  override def explain(table: Table): String = {
    val explain = getStreamTableEnvClass.getMethod("explain", classOf[Table])
    explain.invoke(tableEnv, table).asInstanceOf[String]
  }

  override def explain(table: Table, extended: JBoolean): String = {
    val explain = getStreamTableEnvClass.getMethod("explain", classOf[Table], classOf[JBoolean])
    explain.invoke(tableEnv, table, extended).asInstanceOf[String]
  }

  override def explain(extended: JBoolean): String = {
    val explain = getStreamTableEnvClass.getMethod("explain", classOf[JBoolean])
    explain.invoke(tableEnv, extended).asInstanceOf[String]
  }

  override def fromTableSource(source: TableSource[_]): Table = {
    val fromTableSource = getStreamTableEnvClass.getMethod("fromTableSource", classOf[TableSource[_]])
    fromTableSource.invoke(tableEnv, source).asInstanceOf[Table]
  }

  override def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def fromChangelogStream(dataStream: DataStream[Row]): Table = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema, changelogMode: ChangelogMode): Table = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def createTemporaryView[T](path: String, dataStream: DataStream[T], schema: Schema): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def toDataStream(table: Table): DataStream[Row] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): DataStream[T] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def toChangelogStream(table: Table): DataStream[Row] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def toChangelogStream(table: Table, targetSchema: Schema, changelogMode: ChangelogMode): DataStream[Row] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def useModules(strings: String*): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def listFullModules(): Array[ModuleEntry] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def createTable(path: String, descriptor: TableDescriptor): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def createTemporaryTable(path: String, descriptor: TableDescriptor): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def $getStreamGraph(clearTransformations: Boolean): StreamGraph = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def from(descriptor: TableDescriptor): Table = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def sqlUpdate(stmt: String): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def listTables(s: String, s1: String): Array[String] = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def loadPlan(planReference: PlanReference): CompiledPlan = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def compilePlanSql(s: String): CompiledPlan = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))
}
