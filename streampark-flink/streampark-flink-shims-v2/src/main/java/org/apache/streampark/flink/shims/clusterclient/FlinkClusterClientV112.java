package org.apache.streampark.flink.shims.clusterclient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("unchecked")
public class FlinkClusterClientV112<T> implements FlinkClusterClient {

  private final ClusterClient<T> clusterClient;

  public FlinkClusterClientV112(ClusterClient<T> clusterClient) {
    this.clusterClient = clusterClient;
  }

  @Override
  public CompletableFuture<String> triggerSavepoint(JobID jobID, String savepointDir) {
    try {
      Method triggerSavepoint =
          getClusterClientClass().getMethod("triggerSavepoint", JobID.class, String.class);
      return (CompletableFuture<String>)
          triggerSavepoint.invoke(clusterClient, jobID, savepointDir);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("TriggerSavepoint operation failed, job: %s", jobID.toHexString()), e);
    }
  }

  @Override
  public CompletableFuture<String> cancelWithSavepoint(JobID jobID, String savepointDir) {
    try {
      Method cancelWithSavepoint =
          getClusterClientClass().getMethod("cancelWithSavepoint", JobID.class, String.class);
      return (CompletableFuture<String>)
          cancelWithSavepoint.invoke(clusterClient, jobID, savepointDir);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("CancelWithSavepoint operation failed, job: %s", jobID.toHexString()), e);
    }
  }

  @Override
  public CompletableFuture<String> stopWithSavepoint(
      JobID jobID, boolean advanceToEndOfEventTime, String savepointDir) {
    try {
      Method stopWithSavepoint =
          getClusterClientClass()
              .getMethod("stopWithSavepoint", JobID.class, boolean.class, String.class);
      return (CompletableFuture<String>)
          stopWithSavepoint.invoke(clusterClient, jobID, advanceToEndOfEventTime, savepointDir);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("StopWithSavepoint operation failed, job: %s", jobID.toHexString()), e);
    }
  }
}
