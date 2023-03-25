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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableDescriptor}
import org.apache.streampark.flink.shims.base.{FlinkTableInitializer, StreamTableEnvConfig}

import java.lang.{Boolean => JBoolean}

class StreamTableContextV114(override val parameter: ParameterTool, private val streamEnv: StreamExecutionEnvironment, private val tableEnv: StreamTableEnvironment)
  extends StreamTableContextV113(parameter, streamEnv, tableEnv) {

  def this(args: (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment)) = this(args._1, args._2, args._3)

  def this(args: StreamTableEnvConfig) = this(FlinkTableInitializer.initialize(args))

  override def createTable(path: String, descriptor: TableDescriptor): Unit = this.tableEnv.createTable(path, descriptor)

  override def createTemporaryTable(path: String, descriptor: TableDescriptor): Unit = this.tableEnv.createTemporaryTable(path, descriptor)

  override def $getStreamGraph(clearTransformations: Boolean): StreamGraph = this.streamEnv.getStreamGraph(clearTransformations)

  override def from(descriptor: TableDescriptor): Table = this.tableEnv.from(descriptor)

  override def $getStreamGraph(jobName: String): StreamGraph = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def $getStreamGraph(jobName: String, clearTransformations: JBoolean): StreamGraph = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))
}
