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
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sources.TableSource
import org.apache.streampark.flink.shims.base.{FlinkTableInitializer, TableEnvConfig}

import java.lang.{Boolean => JBoolean}

class FlinkTableContextV112(override val parameter: ParameterTool, private val tableEnv: TableEnvironment) extends FlinkTableContext(parameter, tableEnv) {
  def this(args: (ParameterTool, TableEnvironment)) = this(args._1, args._2)
  def this(args: TableEnvConfig) = this(FlinkTableInitializer.initialize(args))

  override def insertInto(targetPath: String, table: Table): Unit = {
    val insertInto = getTableEnvClass.getMethod("insertInto", classOf[String], classOf[Table])
    insertInto.invoke(tableEnv, targetPath, table)
  }

  override def explain(table: Table): String = {
    val explain = getTableEnvClass.getMethod("explain", classOf[Table])
    explain.invoke(tableEnv, table).asInstanceOf[String]
  }

  override def explain(table: Table, extended: JBoolean): String = {
    val explain = getTableEnvClass.getMethod("explain", classOf[Table], classOf[JBoolean])
    explain.invoke(tableEnv, table, extended).asInstanceOf[String]
  }

  override def explain(extended: JBoolean): String = {
    val explain = getTableEnvClass.getMethod("explain", classOf[JBoolean])
    explain.invoke(tableEnv, extended).asInstanceOf[String]
  }

  override def fromTableSource(source: TableSource[_]): Table = {
    val fromTableSource = getTableEnvClass.getMethod("fromTableSource", classOf[TableSource[_]])
    fromTableSource.invoke(tableEnv, source).asInstanceOf[Table]
  }

  override def sqlUpdate(stmt: String): Unit = {
    val sqlUpdate = getTableEnvClass.getMethod("sqlUpdate", classOf[String])
    sqlUpdate.invoke(tableEnv, stmt)
  }
}
