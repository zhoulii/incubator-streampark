package org.apache.streampark.flink.shims.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

public class FlinkKubeClientV116 extends FlinkKubeClientV115 {
  public FlinkKubeClientV116(FlinkKubeClient flinkKubeClient) {
    super(flinkKubeClient);
  }
}
