package org.apache.streampark.flink.shims.clusterclient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("unchecked")
public class FlinkClusterClientV115<T> extends FlinkClusterClientV114<T> {

  private final ClusterClient<T> clusterClient;

  public FlinkClusterClientV115(ClusterClient<T> clusterClient) {
    super(clusterClient);
    this.clusterClient = clusterClient;
  }

  @Override
  public CompletableFuture<String> triggerSavepoint(JobID jobID, String savepointDir) {
    try {
      Class<?> savepointFormatTypeClass =
          Class.forName("org.apache.flink.core.execution.SavepointFormatType");
      Object defaultSavepointFormatTypeClass =
          savepointFormatTypeClass.getDeclaredField("DEFAULT").get(savepointFormatTypeClass);

      Method triggerSavepoint =
          getClusterClientClass()
              .getMethod("triggerSavepoint", JobID.class, String.class, savepointFormatTypeClass);
      return (CompletableFuture<String>)
          triggerSavepoint.invoke(
              clusterClient, jobID, savepointDir, defaultSavepointFormatTypeClass);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("TriggerSavepoint operation failed, job: %s", jobID.toHexString()), e);
    }
  }

  @Override
  public CompletableFuture<String> cancelWithSavepoint(JobID jobID, String savepointDir) {
    try {
      Class<?> savepointFormatTypeClass =
          Class.forName("org.apache.flink.core.execution.SavepointFormatType");
      Object defaultSavepointFormatTypeClass =
          savepointFormatTypeClass.getDeclaredField("DEFAULT").get(savepointFormatTypeClass);

      Method cancelWithSavepoint =
          getClusterClientClass()
              .getMethod(
                  "cancelWithSavepoint", JobID.class, String.class, savepointFormatTypeClass);
      return (CompletableFuture<String>)
          cancelWithSavepoint.invoke(
              clusterClient, jobID, savepointDir, defaultSavepointFormatTypeClass);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("CancelWithSavepoint operation failed, job: %s", jobID.toHexString()), e);
    }
  }

  @Override
  public CompletableFuture<String> stopWithSavepoint(
      JobID jobID, boolean advanceToEndOfEventTime, String savepointDir) {
    try {
      Class<?> savepointFormatTypeClass =
          Class.forName("org.apache.flink.core.execution.SavepointFormatType");
      Object defaultSavepointFormatTypeClass =
          savepointFormatTypeClass.getDeclaredField("DEFAULT").get(savepointFormatTypeClass);

      Method stopWithSavepoint =
          getClusterClientClass()
              .getMethod(
                  "stopWithSavepoint",
                  JobID.class,
                  boolean.class,
                  String.class,
                  savepointFormatTypeClass);
      return (CompletableFuture<String>)
          stopWithSavepoint.invoke(
              clusterClient,
              jobID,
              advanceToEndOfEventTime,
              savepointDir,
              defaultSavepointFormatTypeClass);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("CancelWithSavepoint operation failed, job: %s", jobID.toHexString()), e);
    }
  }
}
