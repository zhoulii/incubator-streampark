package org.apache.streampark.common.util

import org.apache.streampark.common.conf.FlinkConfigConst
import org.apache.streampark.common.flink.compatible.TimeUtils

import java.io.File
import java.time.Duration
import java.util

class FlinkUtils {

  def getFlinkDistJar(flinkHome: String): String = {
    new File(s"$flinkHome/lib").list().filter(_.matches("flink-dist.*\\.jar")) match {
      case Array() =>
        throw new IllegalArgumentException(
          s"[StreamPark] can no found flink-dist jar in $flinkHome/lib")
      case array if array.length == 1 => s"$flinkHome/lib/${array.head}"
      case more =>
        throw new IllegalArgumentException(
          s"[StreamPark] found multiple flink-dist jar in $flinkHome/lib,[${more.mkString(",")}]")
    }
  }

  def isCheckpointEnabled(map: util.Map[String, String]): Boolean = {
    val checkpointInterval: Duration =
      TimeUtils.parseDuration(map.getOrDefault(FlinkConfigConst.CHECKPOINTING_INTERVAL.key, "0ms"))
    checkpointInterval.toMillis > 0
  }

}
