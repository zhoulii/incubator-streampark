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
package org.apache.streampark.common.conf

import java.time.Duration

object FlinkConfigConst {

  implicit val (prefix, prop) = ("", null)

  /** The maximum number of completed checkpoints to retain. */
  val MAX_RETAINED_CHECKPOINTS: ConfigOption[Integer] = ConfigOption(
    key = "state.checkpoints.num-retained",
    required = false,
    classType = classOf[Integer],
    defaultValue = 1)

  /**
   * The default directory for savepoints. Used by the state backends that write savepoints to" + "
   * file systems (HashMapStateBackend, EmbeddedRocksDBStateBackend).
   */
  val SAVEPOINT_DIRECTORY: ConfigOption[String] = ConfigOption(
    key = "state.savepoints.dir",
    required = false,
    classType = classOf[String],
    defaultValue = null)

  /**
   * The address that should be used by clients to connect to the server. Attention: This option is
   * respected only if the high-availability configuration is NONE.
   */
  val ADDRESS: ConfigOption[String] = ConfigOption(
    key = "rest.address",
    required = false,
    classType = classOf[String],
    defaultValue = null)

  /**
   * "The port that the client connects to. If %s has not been specified, then the REST server will
   * bind to this port. Attention: This option is respected only if the high-availability
   * configuration is NONE."
   */
  val PORT: ConfigOption[Int] =
    ConfigOption(key = "rest.port", required = false, classType = classOf[Int], defaultValue = 8081)

  /**
   * Allow to skip savepoint state that cannot be restored Allow this if you removed an operator
   * from your pipeline after the savepoint was triggered.
   */
  val SAVEPOINT_IGNORE_UNCLAIMED_STATE: ConfigOption[Boolean] =
    ConfigOption(
      key = "execution.savepoint.ignore-unclaimed-state",
      required = false,
      classType = classOf[Int],
      defaultValue = false)

  /** Dictionary for JobManager to store the archives of completed jobs. */
  val ARCHIVE_DIR: ConfigOption[String] = ConfigOption(
    key = "jobmanager.archive.fs.dir",
    required = false,
    classType = classOf[String],
    defaultValue = null)

  /**
   * first check the user code jar (child-first) or the application classpath (parent-first). The
   * default settings indicate to load classes first from the user code jar, which means that user
   * code jars can include and load different dependencies than Flink uses (transitively).
   */
  val CLASSLOADER_RESOLVE_ORDER: ConfigOption[String] = ConfigOption(
    key = "classloader.resolve-order",
    required = false,
    classType = classOf[String],
    defaultValue = null)

  /** Gets the interval in which checkpoints are periodically scheduled. */
  val CHECKPOINTING_INTERVAL: ConfigOption[Duration] = ConfigOption(
    key = "execution.checkpointing.interval",
    required = false,
    classType = classOf[Duration],
    defaultValue = null)

}
