package org.apache.streampark.flink.shims;

import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClient;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV112;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV113;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV114;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV115;
import org.apache.streampark.flink.shims.clusterclient.FlinkClusterClientV116;
import org.apache.streampark.flink.shims.context.FlinkTableContext;
import org.apache.streampark.flink.shims.context.StreamTableContext;
import org.apache.streampark.flink.shims.ext.EnhancedTableConversions;
import org.apache.streampark.flink.shims.ext.TableConversionsV112;
import org.apache.streampark.flink.shims.ext.TableConversionsV115;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClient;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV112;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV113;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV114;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV115;
import org.apache.streampark.flink.shims.kubeclient.FlinkKubeClientV116;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkShimLoader {

  public static final String FLINK_VERSION_V1_12 = "1.12";
  public static final String FLINK_VERSION_V1_13 = "1.13";
  public static final String FLINK_VERSION_V1_14 = "1.14";
  public static final String FLINK_VERSION_V1_15 = "1.15";
  public static final String FLINK_VERSION_V1_16 = "1.16";

  private FlinkShimLoader() {}

  public static <T> FlinkClusterClient loadFlinkClient(ClusterClient<T> clusterClient) {
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
        return new FlinkClusterClientV112<>(clusterClient);
      case FLINK_VERSION_V1_13:
        return new FlinkClusterClientV113<>(clusterClient);
      case FLINK_VERSION_V1_14:
        return new FlinkClusterClientV114<>(clusterClient);
      case FLINK_VERSION_V1_15:
        return new FlinkClusterClientV115<>(clusterClient);
      case FLINK_VERSION_V1_16:
        return new FlinkClusterClientV116<>(clusterClient);
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }

  public static FlinkKubeClient loadFlinkKubeClient(
      org.apache.flink.kubernetes.kubeclient.FlinkKubeClient flinkKubeClient) {
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
        return new FlinkKubeClientV112(flinkKubeClient);
      case FLINK_VERSION_V1_13:
        return new FlinkKubeClientV113(flinkKubeClient);
      case FLINK_VERSION_V1_14:
        return new FlinkKubeClientV114(flinkKubeClient);
      case FLINK_VERSION_V1_15:
        return new FlinkKubeClientV115(flinkKubeClient);
      case FLINK_VERSION_V1_16:
        return new FlinkKubeClientV116(flinkKubeClient);
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }

  public static FlinkTableContext loadTableContext(ParameterTool pt, TableEnvironment tEnv)
      throws Exception {
    Class<?> tableContext = null;
    Object o = null;
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.FlinkTableContextV112");
        o =
            tableContext
                .getConstructor(ParameterTool.class, TableEnvironment.class)
                .newInstance(pt, tEnv);
        return (FlinkTableContext) o;
      case FLINK_VERSION_V1_13:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.FlinkTableContextV113");
        o =
            tableContext
                .getConstructor(ParameterTool.class, TableEnvironment.class)
                .newInstance(pt, tEnv);
        return (FlinkTableContext) o;
      case FLINK_VERSION_V1_14:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.FlinkTableContextV114");
        o =
            tableContext
                .getConstructor(ParameterTool.class, TableEnvironment.class)
                .newInstance(pt, tEnv);
        return (FlinkTableContext) o;
      case FLINK_VERSION_V1_15:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.FlinkTableContextV115");
        o =
            tableContext
                .getConstructor(ParameterTool.class, TableEnvironment.class)
                .newInstance(pt, tEnv);
        return (FlinkTableContext) o;
      case FLINK_VERSION_V1_16:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.FlinkTableContextV116");
        o =
            tableContext
                .getConstructor(ParameterTool.class, TableEnvironment.class)
                .newInstance(pt, tEnv);
        return (FlinkTableContext) o;
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }

  public static StreamTableContext loadStreamTableContext(
      ParameterTool pt, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
      throws Exception {
    Class<?> tableContext = null;
    Object o = null;
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.StreamTableContextV112");
        o =
            tableContext
                .getConstructor(
                    ParameterTool.class,
                    StreamExecutionEnvironment.class,
                    StreamTableEnvironment.class)
                .newInstance(pt, env, tEnv);
        return (StreamTableContext) o;
      case FLINK_VERSION_V1_13:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.StreamTableContextV113");
        o =
            tableContext
                .getConstructor(
                    ParameterTool.class,
                    StreamExecutionEnvironment.class,
                    StreamTableEnvironment.class)
                .newInstance(pt, env, tEnv);
        return (StreamTableContext) o;
      case FLINK_VERSION_V1_14:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.StreamTableContextV114");
        o =
            tableContext
                .getConstructor(
                    ParameterTool.class,
                    StreamExecutionEnvironment.class,
                    StreamTableEnvironment.class)
                .newInstance(pt, env, tEnv);
        return (StreamTableContext) o;
      case FLINK_VERSION_V1_15:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.StreamTableContextV115");
        o =
            tableContext
                .getConstructor(
                    ParameterTool.class,
                    StreamExecutionEnvironment.class,
                    StreamTableEnvironment.class)
                .newInstance(pt, env, tEnv);
        return (StreamTableContext) o;
      case FLINK_VERSION_V1_16:
        tableContext =
            Class.forName("org.apache.streampark.flink.shims.context.StreamTableContextV116");
        o =
            tableContext
                .getConstructor(
                    ParameterTool.class,
                    StreamExecutionEnvironment.class,
                    StreamTableEnvironment.class)
                .newInstance(pt, env, tEnv);
        return (StreamTableContext) o;
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }

  public static EnhancedTableConversions loadTableConversions(Table table) throws Exception {
    Class<?> tableContext = null;
    Object o = null;
    switch (FlinkVersion.current().toString()) {
      case FLINK_VERSION_V1_12:
      case FLINK_VERSION_V1_13:
      case FLINK_VERSION_V1_14:
        return new TableConversionsV112(table);
      case FLINK_VERSION_V1_15:
      case FLINK_VERSION_V1_16:
        return new TableConversionsV115(table);
      default:
        throw new RuntimeException(
            "Unsupported Flink version " + FlinkVersion.current().toString());
    }
  }
}
