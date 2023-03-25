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

package org.apache.streampark.flink.shims.ext

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.bridge.scala.{TableConversions => FlinkTableConversions}
import org.apache.flink.table.api.{Table => FlinkTable}
import org.apache.streampark.flink.shims.context.StreamTableContext

abstract class EnhancedTableConversions(table: FlinkTable) extends FlinkTableConversions(table) {

  def \\[T: TypeInformation]: DataSet[T] = {
    val clazz = Class.forName("org.apache.flink.table.api.bridge.scala.TableConversions")
    val constructor = clazz.getConstructor(classOf[FlinkTable])
    val conversions = constructor.newInstance(table)
    clazz.getMethod("toDataSet").invoke(conversions).asInstanceOf[DataSet[T]]
  }

  def >>[T: TypeInformation](implicit context: StreamTableContext): DataStream[T] = {
    context.isConvertedToDataStream = true
    super.toAppendStream.javaStream
  }

  def <<[T: TypeInformation](implicit context: StreamTableContext): DataStream[(Boolean, T)] = {
    context.isConvertedToDataStream = true
    super.toRetractStream.javaStream
  }

  def toAppendStream[T](implicit typeInfo: TypeInformation[T], context: StreamTableContext): DataStream[T] = {
    context.isConvertedToDataStream = true
    super.toAppendStream.javaStream
  }

  def toRetractStream[T](implicit typeInfo: TypeInformation[T], context: StreamTableContext): DataStream[(Boolean, T)] = {
    context.isConvertedToDataStream = true
    super.toRetractStream.javaStream
  }

}
