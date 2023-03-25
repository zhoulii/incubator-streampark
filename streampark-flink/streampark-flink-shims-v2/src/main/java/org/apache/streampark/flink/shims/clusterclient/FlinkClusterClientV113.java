package org.apache.streampark.flink.shims.clusterclient;

import org.apache.flink.client.program.ClusterClient;

@SuppressWarnings("unchecked")
public class FlinkClusterClientV113<T> extends FlinkClusterClientV112<T> {
  public FlinkClusterClientV113(ClusterClient<T> clusterClient) {
    super(clusterClient);
  }
}
