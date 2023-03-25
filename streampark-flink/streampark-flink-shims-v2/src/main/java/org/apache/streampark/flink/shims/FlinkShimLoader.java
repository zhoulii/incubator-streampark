package org.apache.streampark.flink.shims;

import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClient;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV112;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV113;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV114;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV115;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV116;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClient;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV112;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV113;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV114;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV115;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV116;

import org.apache.flink.FlinkVersion;
import org.apache.flink.client.program.ClusterClient;

public class FlinkShimLoader {

  public static final String FLINK_VERSION_V1_12 = "1.12";
  public static final String FLINK_VERSION_V1_13 = "1.13";
  public static final String FLINK_VERSION_V1_14 = "1.14";
  public static final String FLINK_VERSION_V1_15 = "1.15";
  public static final String FLINK_VERSION_V1_16 = "1.16";

  private FlinkShimLoader() {}

  public static <T> FlinkClusterClient loadFlinkClient(ClusterClient<T> clusterClient) {
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
        return new FlinkClusterClientV112<>(clusterClient);
      case FLINK_VERSION_V1_13:
        return new FlinkClusterClientV113<>(clusterClient);
      case FLINK_VERSION_V1_14:
        return new FlinkClusterClientV114<>(clusterClient);
      case FLINK_VERSION_V1_15:
        return new FlinkClusterClientV115<>(clusterClient);
      case FLINK_VERSION_V1_16:
        return new FlinkClusterClientV116<>(clusterClient);
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }

  public static FlinkKubeClient loadFlinkKubeClient(
      org.apache.flink.kubernetes.kubeclient.FlinkKubeClient flinkKubeClient) {
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
        return new FlinkKubeClientV112(flinkKubeClient);
      case FLINK_VERSION_V1_13:
        return new FlinkKubeClientV113(flinkKubeClient);
      case FLINK_VERSION_V1_14:
        return new FlinkKubeClientV114(flinkKubeClient);
      case FLINK_VERSION_V1_15:
        return new FlinkKubeClientV115(flinkKubeClient);
      case FLINK_VERSION_V1_16:
        return new FlinkKubeClientV116(flinkKubeClient);
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }
}
