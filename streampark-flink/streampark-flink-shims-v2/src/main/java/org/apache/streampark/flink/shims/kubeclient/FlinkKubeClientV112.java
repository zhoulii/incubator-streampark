package org.apache.streampark.flink.shims.kubeclient;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;

import java.lang.reflect.Method;
import java.util.Optional;

@SuppressWarnings("unchecked")
public class FlinkKubeClientV112 implements FlinkKubeClient {

  private org.apache.flink.kubernetes.kubeclient.FlinkKubeClient flinkKubeClient;

  public FlinkKubeClientV112(
      org.apache.flink.kubernetes.kubeclient.FlinkKubeClient flinkKubeClient) {
    this.flinkKubeClient = flinkKubeClient;
  }

  @Override
  public Optional<KubernetesService> getService(String serviceName) {
    try {
      Method getRestService = getFlinkKubeClientClass().getMethod("getRestService", String.class);
      return (Optional<KubernetesService>) getRestService.invoke(flinkKubeClient, serviceName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get KubernetesService with name %s", serviceName), e);
    }
  }
}
