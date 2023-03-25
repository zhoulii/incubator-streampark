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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{Schema, Table}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.module.ModuleEntry
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row
import org.apache.streampark.flink.shims.base.{FlinkTableInitializer, StreamTableEnvConfig}

class StreamTableContextV113(override val parameter: ParameterTool, private val streamEnv: StreamExecutionEnvironment, private val tableEnv: StreamTableEnvironment)
  extends StreamTableContextV112(parameter, streamEnv, tableEnv) {

  def this(args: (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment)) = this(args._1, args._2, args._3)

  def this(args: StreamTableEnvConfig) = this(FlinkTableInitializer.initialize(args))

  override def fromDataStream[T](dataStream: DataStream[T], schema: Schema): Table = this.tableEnv.fromDataStream(dataStream, schema)

  override def fromChangelogStream(dataStream: DataStream[Row]): Table = this.tableEnv.fromChangelogStream(dataStream)

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema): Table = this.tableEnv.fromChangelogStream(dataStream, schema)

  override def fromChangelogStream(dataStream: DataStream[Row], schema: Schema, changelogMode: ChangelogMode): Table = this.tableEnv.fromChangelogStream(dataStream, schema, changelogMode)

  override def createTemporaryView[T](path: String, dataStream: DataStream[T], schema: Schema): Unit = this.tableEnv.createTemporaryView(path, dataStream, schema)

  override def toDataStream(table: Table): DataStream[Row] = this.tableEnv.toDataStream(table)

  override def toDataStream[T](table: Table, targetClass: Class[T]): DataStream[T] = this.tableEnv.toDataStream(table, targetClass)

  override def toDataStream[T](table: Table, targetDataType: AbstractDataType[_]): DataStream[T] = this.tableEnv.toDataStream(table, targetDataType)

  override def toChangelogStream(table: Table): DataStream[Row] = this.tableEnv.toChangelogStream(table)

  override def toChangelogStream(table: Table, targetSchema: Schema): DataStream[Row] = this.tableEnv.toChangelogStream(table, targetSchema)

  override def toChangelogStream(table: Table, targetSchema: Schema, changelogMode: ChangelogMode): DataStream[Row] = this.tableEnv.toChangelogStream(table, targetSchema, changelogMode)

  override def useModules(strings: String*): Unit = this.tableEnv.useModules(strings: _*)

  override def listFullModules(): Array[ModuleEntry] = this.tableEnv.listFullModules()
}
