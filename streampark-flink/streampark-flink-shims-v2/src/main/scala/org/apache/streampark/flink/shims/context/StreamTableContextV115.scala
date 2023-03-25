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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{CompiledPlan, PlanReference, Table}
import org.apache.flink.table.sources.TableSource
import org.apache.streampark.flink.shims.base.{FlinkTableInitializer, StreamTableEnvConfig}

import java.lang.{Boolean => JBoolean}

class StreamTableContextV115(override val parameter: ParameterTool, private val streamEnv: StreamExecutionEnvironment, private val tableEnv: StreamTableEnvironment)
  extends StreamTableContextV114(parameter, streamEnv, tableEnv) {

  def this(args: (ParameterTool, StreamExecutionEnvironment, StreamTableEnvironment)) = this(args._1, args._2, args._3)

  def this(args: StreamTableEnvConfig) = this(FlinkTableInitializer.initialize(args))

  override def listTables(s: String, s1: String): Array[String] = this.tableEnv.listTables(s, s1)

  override def loadPlan(planReference: PlanReference): CompiledPlan = this.tableEnv.loadPlan(planReference)

  override def compilePlanSql(s: String): CompiledPlan = this.tableEnv.compilePlanSql(s)

  override def sqlUpdate(stmt: String): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def insertInto(targetPath: String, table: Table): Unit = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def explain(table: Table): String = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def explain(table: Table, extended: JBoolean): String = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def explain(extended: JBoolean): String = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

  override def fromTableSource(source: TableSource[_]): Table = throw new RuntimeException(String.format("api not supported in flink %", FlinkVersion.current().toString))

}
