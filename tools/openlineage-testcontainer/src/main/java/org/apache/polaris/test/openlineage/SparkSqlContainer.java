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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.polaris.test.openlineage;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public final class SparkSqlContainer extends GenericContainer<SparkSqlContainer> {
  public static final String STACK_LABEL_KEY = "org.apache.polaris.openlineage.stack";
  public static final String ROLE_LABEL_KEY = "org.apache.polaris.openlineage.role";
  public static final String STACK_LABEL_VALUE = "default";
  public static final String ROLE_LABEL_VALUE = "spark-sql";
  private static final String ICEBERG_PACKAGES =
      "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,"
          + "org.apache.iceberg:iceberg-aws-bundle:1.10.1,"
          + "org.apache.iceberg:iceberg-gcp-bundle:1.10.1,"
          + "org.apache.iceberg:iceberg-azure-bundle:1.10.1";

  private final OpenLineageEnvironment.Config config;

  public SparkSqlContainer(OpenLineageEnvironment.Config config) {
    super(resolveImage());
    this.config = config;
    withAccessToHost(true);
    withLabel(STACK_LABEL_KEY, STACK_LABEL_VALUE);
    withLabel(ROLE_LABEL_KEY, ROLE_LABEL_VALUE);
    withEnv("HOME", "/tmp");
    withCommand("/bin/bash", "-lc", "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && sleep infinity");
    waitingFor(org.testcontainers.containers.wait.strategy.Wait.forListeningPort()
        .withStartupTimeout(Duration.ofMinutes(2)));
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(SparkSqlContainer.class)));
  }

  public Container.ExecResult runSql(String sql) throws IOException, InterruptedException {
    if (!isRunning()) {
      throw new IllegalStateException("Spark SQL container is not running");
    }
    exposePolarisPort();
    return execInContainer(nonInteractiveSparkSqlCommand(sql));
  }

  public String[] interactiveSparkSqlCommand() {
    List<String> command = new ArrayList<>();
    command.add("/opt/spark/bin/spark-sql");
    command.add("--packages");
    command.add(ICEBERG_PACKAGES);
    command.add("--conf");
    command.add("spark.jars.ivy=/tmp/.ivy2");
    command.add("--conf");
    command.add("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.type=rest");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.warehouse=" + config.catalogName());
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.uri=http://host.testcontainers.internal:"
        + config.polarisApiPort()
        + "/api/catalog");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.credential="
        + config.polarisClientId()
        + ':'
        + config.polarisClientSecret());
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.rest.auth.type=oauth2");
    command.add("--conf");
    command.add(
        "spark.sql.catalog.polaris.oauth2-server-uri=http://host.testcontainers.internal:"
            + config.polarisApiPort()
            + "/api/catalog/v1/oauth/tokens");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.rest-metrics-reporting-enabled=false");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.token-refresh-enabled=true");
    command.add("--conf");
    command.add("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation=vended-credentials");
    command.add("--conf");
    command.add("spark.sql.defaultCatalog=polaris");
    return command.toArray(String[]::new);
  }

  public void exposePolarisPort() {
    Testcontainers.exposeHostPorts(config.polarisApiPort());
  }

  private String[] nonInteractiveSparkSqlCommand(String sql) {
    List<String> command = new ArrayList<>(List.of(interactiveSparkSqlCommand()));
    command.add("-e");
    command.add(sql);
    return command.toArray(String[]::new);
  }

  private static DockerImageName resolveImage() {
    return ContainerSpecHelper.containerSpecHelper("spark-sql", SparkSqlContainer.class)
        .dockerImageName(null);
  }
}
