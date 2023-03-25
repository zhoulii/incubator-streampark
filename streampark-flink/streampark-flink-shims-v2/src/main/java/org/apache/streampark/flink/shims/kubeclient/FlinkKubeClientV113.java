package org.apache.streampark.flink.shims.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

public class FlinkKubeClientV113 extends FlinkKubeClientV112 {
  public FlinkKubeClientV113(FlinkKubeClient flinkKubeClient) {
    super(flinkKubeClient);
  }
}
