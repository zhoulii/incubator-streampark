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

package org.apache.streampark.flink.proxy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectOutputStream}
import java.net.URL
import java.util.function.{Function => JavaFunc}
import java.util.regex.Pattern

import scala.collection.mutable.{ListBuffer, Map => MutableMap}

import org.apache.streampark.common.conf.{ConfigConst, FlinkVersion}
import org.apache.streampark.common.util.{ClassLoaderUtils, Logger, Utils}

object FlinkShimsProxy extends Logger {

  private[this] val INCLUDE_PATTERN: Pattern = Pattern.compile(
    "(json4s|jackson)(.*).jar",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private[this] val SHIMS_PATTERN = Pattern.compile(
    "streampark-flink-shims_flink-(1.12|1.13|1.14|1.15|1.16)_(2.11|2.12)-(.*).jar",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private[this] val FLINK_SHIMS_V2: String = "streampark-flink-shims-v2"

  private[this] val SHIMS_CLASS_LOADER_CACHE = MutableMap[String, ClassLoader]()

  private[this] val VERIFY_SQL_CLASS_LOADER_CACHE = MutableMap[String, ClassLoader]()

  private[this] def getFlinkShimsResourcePattern(flinkLargeVersion: String) =
    Pattern.compile(
      s"flink-(.*)-$flinkLargeVersion(.*).jar",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  private[this] def getStreamParkLibPattern(scalaVersion: String): Pattern = Pattern.compile(
    s"streampark-(.*)_$scalaVersion-(.*).jar",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL)

  /**
   * Get shimsClassLoader to execute for scala API
   *
   * @param flinkVersion flinkVersion
   * @param func execute function
   * @tparam T
   * @return
   */
  def proxy[T](flinkVersion: FlinkVersion, func: ClassLoader => T): T = {
    val shimsClassLoader = getFlinkShimsClassLoader(flinkVersion)
    ClassLoaderUtils.runAsClassLoader[T](shimsClassLoader, () => func(shimsClassLoader))
  }

  /**
   * Get shimsClassLoader to execute for java API
   *
   * @param flinkVersion flinkVersion
   * @param func execute function
   * @tparam T
   * @return
   */
  def proxy[T](flinkVersion: FlinkVersion, func: JavaFunc[ClassLoader, T]): T = {
    val shimsClassLoader = getFlinkShimsClassLoader(flinkVersion)
    ClassLoaderUtils.runAsClassLoader[T](shimsClassLoader, () => func(shimsClassLoader))
  }

  // flink 1.12 1.13~1.14 1.15 1.16 parseSql class exist in different dependencies,
  // need to load all flink-table dependencies compatible with different versions
  def getVerifySqlLibClassLoader(flinkVersion: FlinkVersion): ClassLoader = {
    logInfo(s"add verify sql lib,flink version: $flinkVersion")
    VERIFY_SQL_CLASS_LOADER_CACHE.getOrElseUpdate(
      s"${flinkVersion.fullVersion}", {
        val getFlinkTable: File => Boolean = _.getName.startsWith("flink-table")
        // 1) flink/lib/flink-table*
        val libTableURL = getFlinkHomeLib(flinkVersion.flinkHome, "lib", getFlinkTable)

        // 2) After version 1.15 need add flink/opt/flink-table*
        val optTableURL = getFlinkHomeLib(flinkVersion.flinkHome, "opt", getFlinkTable)
        val shimsUrls = ListBuffer[URL](libTableURL ++ optTableURL: _*)

        // 3) add only streampark shims jar
        addShimsUrls(
          flinkVersion,
          file => {
            if (file != null && file.getName.startsWith("streampark-flink-shims")) {
              shimsUrls += file.toURI.toURL
            }
          })
        new ChildFirstClassLoader(
          shimsUrls.toArray,
          Thread.currentThread().getContextClassLoader,
          getFlinkShimsResourcePattern(flinkVersion.majorVersion))
      })
  }

  def addShimsUrls(flinkVersion: FlinkVersion, addShimUrl: File => Unit): Unit = {
    val appHome = System.getProperty(ConfigConst.KEY_APP_HOME)
    require(appHome != null, String.format("%s is not found on System env.", ConfigConst.KEY_APP_HOME))

    val libPath = new File(s"$appHome/lib")
    require(libPath.exists())
    val majorVersion = flinkVersion.majorVersion
    val scalaVersion = flinkVersion.scalaVersion
    val streamParkMatcher = getStreamParkLibPattern(scalaVersion)

    libPath.listFiles().foreach((jar: File) => {
      try {
        val shimsMatcher = SHIMS_PATTERN.matcher(jar.getName)
        if (shimsMatcher.matches()) {
          if (majorVersion == shimsMatcher.group(1) && scalaVersion == shimsMatcher.group(2)) {
            addShimUrl(jar)
          }
        } else if (jar.getName.startsWith(FLINK_SHIMS_V2)) {
          addShimUrl(jar)
        } else {
          if (INCLUDE_PATTERN.matcher(jar.getName).matches()) {
            addShimUrl(jar)
            logInfo(s"include jar lib: ${jar.getName}")
          }
          if (streamParkMatcher.matcher(jar.getName).matches()) {
            addShimUrl(jar)
            logInfo(s"include streampark lib: ${jar.getName}")
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
  }

  /**
   * Get ClassLoader to verify sql
   *
   * @param flinkVersion flinkVersion
   * @param func         execute function
   * @tparam T
   * @return
   */
  def proxyVerifySql[T](flinkVersion: FlinkVersion, func: JavaFunc[ClassLoader, T]): T = {
    val shimsClassLoader = getVerifySqlLibClassLoader(flinkVersion)
    ClassLoaderUtils.runAsClassLoader[T](shimsClassLoader, () => func(shimsClassLoader))
  }

  private[this] def getFlinkShimsClassLoader(flinkVersion: FlinkVersion): ClassLoader = {
    logInfo(s"add flink shims urls classloader,flink version: $flinkVersion")

    SHIMS_CLASS_LOADER_CACHE.getOrElseUpdate(
      s"${flinkVersion.fullVersion}", {
        // 1) flink/lib
        val libURL = getFlinkHomeLib(flinkVersion.flinkHome, "lib", !_.getName.startsWith("log4j"))
        val shimsUrls = ListBuffer[URL](libURL: _*)

        // 2) add all shims jar
        addShimsUrls(
          flinkVersion,
          file => {
            if (file != null) {
              shimsUrls += file.toURI.toURL
            }
          })

        new ChildFirstClassLoader(
          shimsUrls.toArray,
          Thread.currentThread().getContextClassLoader,
          getFlinkShimsResourcePattern(flinkVersion.majorVersion))
      })
  }

  private[this] def getFlinkHomeLib(flinkHome: String, childDir: String, filterFun: File => Boolean): List[URL] = {
    val file = new File(flinkHome, childDir)
    require(file.isDirectory, s"FLINK_HOME $file does not exist")
    file.listFiles.filter(filterFun).map(_.toURI.toURL).toList
  }

  @throws[Exception]
  def getObject[T](loader: ClassLoader, obj: Object): T = {
    val arrayOutputStream = new ByteArrayOutputStream
    val result = Utils.tryWithResource(new ObjectOutputStream(arrayOutputStream))(objectOutputStream => {
      objectOutputStream.writeObject(obj)
      val byteArrayInputStream = new ByteArrayInputStream(arrayOutputStream.toByteArray)
      Utils.tryWithResource(new ClassLoaderObjectInputStream(loader, byteArrayInputStream))(_.readObject)
    })
    result.asInstanceOf[T]
  }

}
