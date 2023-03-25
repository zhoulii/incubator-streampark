package org.apache.streampark.flink.shims.clusterclient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface FlinkClusterClient extends Serializable {
  CompletableFuture<String> triggerSavepoint(JobID jobID, String savepointDir);

  CompletableFuture<String> cancelWithSavepoint(JobID jobID, String savepointDir);

  CompletableFuture<String> stopWithSavepoint(
      JobID jobID, boolean advanceToEndOfEventTime, String savepointDir);

  default Class<?> getClusterClientClass() {
    try {
      return Class.forName("org.apache.flink.client.program.ClusterClient");
    } catch (ClassNotFoundException e) {
      throw new CatalogException("Failed to find class ClusterClient", e);
    }
  }
}
