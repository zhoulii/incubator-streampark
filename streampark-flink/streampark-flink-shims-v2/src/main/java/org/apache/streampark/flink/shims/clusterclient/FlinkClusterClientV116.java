package org.apache.streampark.flink.shims.clusterclient;

import org.apache.flink.client.program.ClusterClient;

@SuppressWarnings("unchecked")
public class FlinkClusterClientV116<T> extends FlinkClusterClientV115<T> {
  public FlinkClusterClientV116(ClusterClient<T> clusterClient) {
    super(clusterClient);
  }
}
