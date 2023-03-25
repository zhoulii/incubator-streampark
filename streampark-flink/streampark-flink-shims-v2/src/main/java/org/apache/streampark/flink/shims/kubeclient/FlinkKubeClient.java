package org.apache.streampark.flink.shims.kubeclient;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;

import java.io.Serializable;
import java.util.Optional;

public interface FlinkKubeClient extends Serializable {

  Optional<KubernetesService> getService(String serviceName);

  default Class<?> getFlinkKubeClientClass() {
    try {
      return Class.forName("org.apache.flink.kubernetes.kubeclient.FlinkKubeClient");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to find class FlinkKubeClient", e);
    }
  }
}
