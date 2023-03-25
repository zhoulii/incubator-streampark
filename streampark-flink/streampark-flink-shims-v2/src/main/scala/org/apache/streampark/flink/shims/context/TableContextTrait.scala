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

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.Catalog
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions._
import org.apache.flink.table.module.Module
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.AbstractDataType

import java.lang
import java.lang.{Boolean => JBoolean}
import java.util.Optional

trait TableContextTrait {

  def fromValues(values: Expression*): Table

  def fromValues(rowType: AbstractDataType[_], values: Expression*): Table

  def fromValues(values: lang.Iterable[_]): Table

  def fromValues(rowType: AbstractDataType[_], values: lang.Iterable[_]): Table

  def registerCatalog(catalogName: String, catalog: Catalog): Unit

  def getCatalog(catalogName: String): Optional[Catalog]

  def loadModule(moduleName: String, module: Module): Unit

  def unloadModule(moduleName: String): Unit

  def createTemporarySystemFunction(name: String, functionClass: Class[_ <: UserDefinedFunction]): Unit

  def createTemporarySystemFunction(name: String, functionInstance: UserDefinedFunction): Unit

  def dropTemporarySystemFunction(name: String): Boolean

  def createFunction(path: String, functionClass: Class[_ <: UserDefinedFunction]): Unit

  def createFunction(path: String, functionClass: Class[_ <: UserDefinedFunction], ignoreIfExists: Boolean): Unit

  def dropFunction(path: String): Boolean

  def createTemporaryFunction(path: String, functionClass: Class[_ <: UserDefinedFunction]): Unit

  def createTemporaryFunction(path: String, functionInstance: UserDefinedFunction): Unit

  def dropTemporaryFunction(path: String): Boolean

  def createTemporaryView(path: String, view: Table): Unit

  def from(path: String): Table

  def listCatalogs(): Array[String]

  def listModules(): Array[String]

  def listDatabases(): Array[String]

  def listTables(): Array[String]

  def listViews(): Array[String]

  def listTemporaryTables(): Array[String]

  def listTemporaryViews(): Array[String]

  def listUserDefinedFunctions(): Array[String]

  def listFunctions(): Array[String]

  def dropTemporaryTable(path: String): Boolean

  def dropTemporaryView(path: String): Boolean

  def explainSql(statement: String, extraDetails: ExplainDetail*): String

  def sqlQuery(query: String): Table

  def executeSql(statement: String): TableResult

  def getCurrentCatalog: String

  def useCatalog(catalogName: String): Unit

  def getCurrentDatabase: String

  def useDatabase(databaseName: String): Unit

  def getConfig: TableConfig

  def createStatementSet(): StatementSet

  def registerFunction(name: String, function: ScalarFunction): Unit

  def registerTable(name: String, table: Table): Unit

  def scan(tablePath: String*): Table

  def getCompletionHints(statement: String, position: Int): Array[String]

  def sqlUpdate(stmt: String): Unit

  def insertInto(targetPath: String, table: Table): Unit

  def explain(table: Table): String

  def explain(table: Table, extended: JBoolean): String

  def explain(extended: JBoolean): String

  def fromTableSource(source: TableSource[_]): Table
}
