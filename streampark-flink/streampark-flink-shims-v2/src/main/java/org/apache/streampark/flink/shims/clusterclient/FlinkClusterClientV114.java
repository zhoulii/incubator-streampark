package org.apache.streampark.flink.shims.clusterclient;

import org.apache.flink.client.program.ClusterClient;

@SuppressWarnings("unchecked")
public class FlinkClusterClientV114<T> extends FlinkClusterClientV113<T> {
  public FlinkClusterClientV114(ClusterClient<T> clusterClient) {
    super(clusterClient);
  }
}
