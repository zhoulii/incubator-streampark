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

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions._
import org.apache.flink.table.module.Module
import org.apache.flink.table.types.AbstractDataType
import org.apache.streampark.common.conf.ConfigConst._
import org.apache.streampark.flink.shims.base.EnhancerImplicit.EnhanceParameterTool
import org.apache.streampark.flink.shims.base.FlinkSqlExecutor

import java.lang
import java.util.Optional

abstract class FlinkTableContext(val parameter: ParameterTool, private val tableEnv: TableEnvironment)
  extends TableContextTrait {

  def start(): JobExecutionResult = {
    val appName = parameter.getAppName(required = true)
    execute(appName)
  }

  def execute(jobName: String): JobExecutionResult = {
    printLogo(s"FlinkTable $jobName Starting...")
    null
  }

  def sql(sql: String = null): Unit = FlinkSqlExecutor.executeSql(sql, parameter, this)

  private[flink] def sqlWithCallBack(sql: String = null)(implicit callback: Unit => String = null): Unit = FlinkSqlExecutor.executeSql(sql, parameter, this)

  def fromValues(values: Expression*): Table = tableEnv.fromValues(values)

  def fromValues(rowType: AbstractDataType[_], values: Expression*): Table = tableEnv.fromValues(rowType, values: _*)

  def fromValues(values: lang.Iterable[_]): Table = tableEnv.fromValues(values)

  def fromValues(rowType: AbstractDataType[_], values: lang.Iterable[_]): Table = tableEnv.fromValues(rowType, values)

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

  def getCurrentCatalog: String = tableEnv.getCurrentCatalog

  def useCatalog(catalogName: String): Unit = tableEnv.useCatalog(catalogName)

  def getCurrentDatabase: String = tableEnv.getCurrentDatabase

  def useDatabase(databaseName: String): Unit = tableEnv.useDatabase(databaseName)

  def getConfig: TableConfig = tableEnv.getConfig

  def createStatementSet(): StatementSet = tableEnv.createStatementSet()

  def registerFunction(name: String, function: ScalarFunction): Unit = tableEnv.registerFunction(name, function)

  def registerTable(name: String, table: Table): Unit = tableEnv.registerTable(name, table)

  def scan(tablePath: String*): Table = tableEnv.scan(tablePath: _*)

  def getCompletionHints(statement: String, position: Int): Array[String] = tableEnv.getCompletionHints(statement, position)

  def getTableEnvClass: Class[_] = try {
    Class.forName("org.apache.flink.table.api.TableEnvironment")
  } catch {
    case e: ClassNotFoundException =>
      throw new RuntimeException("Failed to find class TableEnvironment", e)
  }
}
