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
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Testcontainers-based replacement for the shell launchers used to demo Polaris + OpenLineage +
 * Marquez. This environment expects a local Polaris server to already be running and manages the
 * Marquez + Spark container lifecycle in-process.
 */
public final class OpenLineageEnvironment implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageEnvironment.class);
  private static final int POSTGRES_PORT = 5432;

  private final Config config;
  private final Network network;
  private final GenericContainer<?> postgres;
  private final MarquezContainer marquez;
  private final MarquezWebContainer marquezWeb;
  private final SparkSqlContainer sparkSql;

  public OpenLineageEnvironment() {
    this(Config.defaults());
  }

  @SuppressWarnings("resource")
  public OpenLineageEnvironment(Config config) {
    this.config = config;
    this.network = Network.newNetwork();
    this.postgres =
        new GenericContainer<>("postgres:14")
            .withExposedPorts(POSTGRES_PORT)
            .withNetwork(network)
            .withNetworkAliases("postgres")
            .withEnv("POSTGRES_USER", "marquez")
            .withEnv("POSTGRES_PASSWORD", "marquez")
            .withEnv("POSTGRES_DB", "marquez")
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    this.marquez = new MarquezContainer(network);
    this.marquezWeb = new MarquezWebContainer(network);
    this.sparkSql = new SparkSqlContainer(config);
  }

  public OpenLineageEnvironment start() {
    waitForLocalPolaris();
    LOGGER.info("Starting Marquez PostgreSQL container");
    postgres.start();
    LOGGER.info("Starting Marquez API container");
    marquez.start();
    LOGGER.info("Starting Marquez UI container");
    marquezWeb.start();
    ensureCatalogExists();
    LOGGER.info("Starting Spark SQL container");
    sparkSql.exposePolarisPort();
    sparkSql.start();
    return this;
  }

  public String marquezApiUrl() {
    return marquez.apiUrl();
  }

  public String marquezWebUrl() {
    return marquezWeb.webUrl();
  }

  public String marquezLineageUrl() {
    return marquez.lineageUrl();
  }

  public Container.ExecResult runSparkSql(String sql) throws IOException, InterruptedException {
    return sparkSql.runSql(sql);
  }

  public SparkSqlContainer sparkSqlContainer() {
    return sparkSql;
  }

  @Override
  public void close() {
    sparkSql.close();
    marquezWeb.close();
    marquez.close();
    postgres.close();
    network.close();
  }

  private void waitForLocalPolaris() {
    HttpClient client = HttpClient.newHttpClient();
    URI healthUri =
        URI.create("http://" + config.polarisHost() + ':' + config.polarisHealthPort() + "/q/health");
    long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        HttpResponse<Void> response =
            client.send(
                HttpRequest.newBuilder(healthUri).timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          return;
        }
      } catch (IOException | InterruptedException e) {
        if (e instanceof InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for Polaris health", interrupted);
        }
      }
      try {
        Thread.sleep(2_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for Polaris health", e);
      }
    }
    throw new IllegalStateException("Local Polaris did not become healthy at " + healthUri);
  }

  private void ensureCatalogExists() {
    HttpClient client = HttpClient.newHttpClient();
    String token = obtainAccessToken(client);
    createCatalog(client, token);
    grantCatalogWrite(client, token);
    attachCatalogRole(client, token);
  }

  private String obtainAccessToken(HttpClient client) {
    String credentials =
        Base64.getEncoder()
            .encodeToString(
                (config.polarisClientId() + ':' + config.polarisClientSecret())
                    .getBytes(StandardCharsets.UTF_8));
    HttpRequest request =
        HttpRequest.newBuilder(
                URI.create(
                    "http://" + config.polarisHost() + ':' + config.polarisApiPort()
                        + "/api/catalog/v1/oauth/tokens"))
            .header("Authorization", "Basic " + credentials)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .timeout(Duration.ofSeconds(20))
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    "grant_type=client_credentials&scope="
                        + URLEncoder.encode("PRINCIPAL_ROLE:ALL", StandardCharsets.UTF_8)))
            .build();
    try {
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        throw new IllegalStateException(
            "Failed to obtain Polaris OAuth token: "
                + response.statusCode()
                + ' '
                + response.body());
      }
      String marker = "\"access_token\":\"";
      int start = response.body().indexOf(marker);
      if (start < 0) {
        throw new IllegalStateException("No access token in response: " + response.body());
      }
      int valueStart = start + marker.length();
      int valueEnd = response.body().indexOf('"', valueStart);
      if (valueEnd <= valueStart) {
        throw new IllegalStateException("No access token terminator in response");
      }
      return response.body().substring(valueStart, valueEnd);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed to obtain Polaris access token", e);
    }
  }

  private void createCatalog(HttpClient client, String token) {
    String body =
        """
        {
          "catalog": {
            "name": "%s",
            "type": "INTERNAL",
            "readOnly": false,
            "properties": {
              "default-base-location": "file:///tmp/polaris/"
            },
            "storageConfigInfo": {
              "storageType": "FILE",
              "allowedLocations": [
                "file:///tmp",
                "file:///tmp/polaris/"
              ]
            }
          }
        }
        """
            .formatted(config.catalogName());
    sendManagementRequest(
        client,
        token,
        "POST",
        "/api/management/v1/catalogs",
        body,
        true);
  }

  private void grantCatalogWrite(HttpClient client, String token) {
    sendManagementRequest(
        client,
        token,
        "PUT",
        "/api/management/v1/catalogs/"
            + config.catalogName()
            + "/catalog-roles/catalog_admin/grants",
        "{\"type\":\"catalog\",\"privilege\":\"TABLE_WRITE_DATA\"}",
        false);
  }

  private void attachCatalogRole(HttpClient client, String token) {
    sendManagementRequest(
        client,
        token,
        "PUT",
        "/api/management/v1/principal-roles/service_admin/catalog-roles/" + config.catalogName(),
        "{\"name\":\"catalog_admin\"}",
        false);
  }

  private void sendManagementRequest(
      HttpClient client, String token, String method, String path, String body, boolean allowConflict) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(
                URI.create("http://" + config.polarisHost() + ':' + config.polarisApiPort() + path))
            .header("Authorization", "Bearer " + token)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(20));
    builder.method(method, HttpRequest.BodyPublishers.ofString(body));
    try {
      HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      int status = response.statusCode();
      if (allowConflict && status == 409) {
        return;
      }
      if (status < 200 || status >= 300) {
        throw new IllegalStateException(
            "Polaris management request failed: " + method + ' ' + path + ' ' + response.body());
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed Polaris management request " + method + ' ' + path, e);
    }
  }

  public record Config(
      String polarisHost,
      int polarisApiPort,
      int polarisHealthPort,
      String polarisRealm,
      String polarisClientId,
      String polarisClientSecret,
      String polarisProducerUri,
      String catalogName) {
    public static Config defaults() {
      return new Config(
          "localhost",
          8181,
          8182,
          "POLARIS",
          "root",
          "s3cr3t",
          "http://localhost:8181",
          "manual_spark");
    }
  }
}
