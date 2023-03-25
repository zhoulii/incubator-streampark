package org.apache.streampark.flink.shims.kubeclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;

import java.lang.reflect.Method;
import java.util.Optional;

@SuppressWarnings("unchecked")
public class FlinkKubeClientV115 extends FlinkKubeClientV114 {
  private final FlinkKubeClient flinkKubeClient;

  public FlinkKubeClientV115(FlinkKubeClient flinkKubeClient) {
    super(flinkKubeClient);
    this.flinkKubeClient = flinkKubeClient;
  }

  @Override
  public Optional<KubernetesService> getService(String serviceName) {
    try {
      Class<?> serviceTypeClass =
          Class.forName(
              "org.apache.flink.kubernetes.kubeclient.resources.KubernetesService.ServiceType");
      Object restService = serviceTypeClass.getEnumConstants()[0];

      Method getRestService =
          getFlinkKubeClientClass().getMethod("getRestService", serviceTypeClass, String.class);
      return (Optional<KubernetesService>)
          getRestService.invoke(flinkKubeClient, restService, serviceName);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get KubernetesService with name %s", serviceName), e);
    }
  }
}
