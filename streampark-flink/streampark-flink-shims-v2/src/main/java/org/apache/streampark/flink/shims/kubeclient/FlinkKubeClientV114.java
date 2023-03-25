package org.apache.streampark.flink.shims.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

public class FlinkKubeClientV114 extends FlinkKubeClientV113 {
  public FlinkKubeClientV114(FlinkKubeClient flinkKubeClient) {
    super(flinkKubeClient);
  }
}
