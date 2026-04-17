/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.test.openlineage;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

/** Small CLI for local OpenLineage + Marquez demo workflows. */
public final class OpenLineageCliMain {
  private static final String DEFAULT_SQL =
      """
      CREATE NAMESPACE IF NOT EXISTS db_src;
      CREATE NAMESPACE IF NOT EXISTS db_out;

      DROP TABLE IF EXISTS db_out.sales_report;
      DROP TABLE IF EXISTS db_src.orders_raw;

      CREATE TABLE db_src.orders_raw USING iceberg AS
      SELECT 1 AS order_id, 101 AS prod_id, 2 AS qty
      UNION ALL
      SELECT 2, 102, 1;

      CREATE TABLE db_out.sales_report USING iceberg AS
      SELECT prod_id, SUM(qty) AS total_qty
      FROM db_src.orders_raw
      GROUP BY prod_id;
      """;

  private OpenLineageCliMain() {}

  public static void main(String[] args) throws Exception {
    String command = args.length == 0 ? "start" : args[0];
    switch (command) {
      case "start" -> runStart();
      case "demo" -> runSql(DEFAULT_SQL);
      case "shell" -> runShell();
      case "sql" -> runSql(joinArgs(args, 1));
      case "help" -> printUsage();
      default -> throw new IllegalArgumentException("Unknown command: " + command);
    }
  }

  private static void runStart() throws Exception {
    OpenLineageEnvironment env = new OpenLineageEnvironment().start();
    Runtime.getRuntime().addShutdownHook(new Thread(env::close, "openlineage-env-shutdown"));
    printConnectionInfo(env);
    System.out.println("Containers are running. Press Ctrl-C to stop them.");
    new CountDownLatch(1).await();
  }

  private static void runSql(String sql) throws Exception {
    if (sql.isBlank()) {
      throw new IllegalArgumentException("No SQL provided");
    }
    try (OpenLineageEnvironment env = new OpenLineageEnvironment().start()) {
      Container.ExecResult result = env.runSparkSql(sql);
      if (!result.getStdout().isBlank()) {
        System.out.println(result.getStdout());
      }
      if (!result.getStderr().isBlank()) {
        System.err.println(result.getStderr());
      }
      if (result.getExitCode() != 0) {
        throw new IllegalStateException("spark-sql exited with code " + result.getExitCode());
      }
      printConnectionInfo(env);
    }
  }

  private static void runShell() throws Exception {
    String runningSparkContainerId = findRunningSparkContainerId();
    if (runningSparkContainerId != null) {
      printConnectionInfo();
      Testcontainers.exposeHostPorts(OpenLineageEnvironment.Config.defaults().polarisApiPort());
      runInteractiveShell(runningSparkContainerId);
      return;
    }

    try (OpenLineageEnvironment env = new OpenLineageEnvironment().start()) {
      printConnectionInfo(env);
      runInteractiveShell(env.sparkSqlContainer().getContainerId());
    }
  }

  private static void runInteractiveShell(String sparkContainerId) throws Exception {
    Process process = new ProcessBuilder(dockerExecCommand(sparkContainerId)).inheritIO().start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new IllegalStateException("interactive spark-sql exited with code " + exitCode);
    }
  }

  private static String[] dockerExecCommand(String sparkContainerId) {
    String[] sparkSqlCommand =
        new SparkSqlContainer(OpenLineageEnvironment.Config.defaults()).interactiveSparkSqlCommand();
    boolean hasConsole = System.console() != null;
    String[] command = new String[(hasConsole ? 4 : 3) + sparkSqlCommand.length];
    command[0] = "docker";
    command[1] = "exec";
    command[2] = hasConsole ? "-it" : "-i";
    command[hasConsole ? 3 : 2] = sparkContainerId;
    System.arraycopy(
        sparkSqlCommand,
        0,
        command,
        hasConsole ? 4 : 3,
        sparkSqlCommand.length);
    return command;
  }

  private static String findRunningSparkContainerId() throws Exception {
    String labelledContainerId =
        firstMatchingContainerId(
            "label="
                + SparkSqlContainer.STACK_LABEL_KEY
                + "="
                + SparkSqlContainer.STACK_LABEL_VALUE,
            "label="
                + SparkSqlContainer.ROLE_LABEL_KEY
                + "="
                + SparkSqlContainer.ROLE_LABEL_VALUE);
    if (labelledContainerId != null) {
      return labelledContainerId;
    }

    // Fallback for a stack started before the labeling change.
    return firstMatchingContainerId("ancestor=apache/spark:3.5.8-java17-python3");
  }

  private static String firstMatchingContainerId(String... filters) throws Exception {
    List<String> command = new ArrayList<>();
    command.add("docker");
    command.add("ps");
    for (String filter : filters) {
      command.add("--filter");
      command.add(filter);
    }
    command.add("--filter");
    command.add("status=running");
    command.add("--format");
    command.add("{{.ID}}");

    Process process = new ProcessBuilder(command).start();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String firstLine = reader.readLine();
      int exitCode = process.waitFor();
      if (exitCode != 0 || firstLine == null || firstLine.isBlank()) {
        return null;
      }
      return firstLine.trim();
    }
  }

  private static void printConnectionInfo() {
    System.out.println("Marquez API: http://localhost:5000");
    System.out.println("Marquez UI: http://localhost:3000");
    System.out.println("Marquez Lineage Endpoint: http://localhost:5000/api/v1/lineage");
  }

  private static void printConnectionInfo(OpenLineageEnvironment env) {
    System.out.println("Marquez API: " + env.marquezApiUrl());
    System.out.println("Marquez UI: " + env.marquezWebUrl());
    System.out.println("Marquez Lineage Endpoint: " + env.marquezLineageUrl());
  }

  private static String joinArgs(String[] args, int startIndex) {
    if (args.length <= startIndex) {
      return "";
    }
    return String.join(" ", Arrays.copyOfRange(args, startIndex, args.length));
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  ./gradlew :polaris-openlineage-testcontainer:run");
    System.out.println("  ./gradlew :polaris-openlineage-testcontainer:run --args='start'");
    System.out.println("  ./gradlew :polaris-openlineage-testcontainer:run --args='demo'");
    System.out.println("  ./gradlew :polaris-openlineage-testcontainer:run --args='shell'");
    System.out.println("  ./gradlew :polaris-openlineage-testcontainer:run --args='sql SHOW TABLES IN db_src;'");
  }
}
