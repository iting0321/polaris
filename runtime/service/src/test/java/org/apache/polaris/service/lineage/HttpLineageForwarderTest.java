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
package org.apache.polaris.service.lineage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.AuthConfiguration;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.AuthType;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.FailureMode;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.TargetConfiguration;
import org.junit.jupiter.api.Test;

class HttpLineageForwarderTest {
  private static final byte[] RAW_EVENT =
      "{\"eventType\":\"COMPLETE\",\"run\":{\"runId\":\"run-1\"}}".getBytes(StandardCharsets.UTF_8);

  @Test
  void disabledForwarderDoesNotSendRequests() {
    HttpClient httpClient = mock(HttpClient.class);
    HttpLineageForwarder forwarder =
        new HttpLineageForwarder(
            config(false, Set.of("marquez"), Map.of("marquez", target(URI.create("http://x")))),
            httpClient);

    forwarder.forwardRawEvent(RAW_EVENT);

    verifyNoInteractions(httpClient);
  }

  @Test
  void forwardsRawEventWithApiKeyAuth() throws Exception {
    AtomicReference<String> body = new AtomicReference<>();
    AtomicReference<String> auth = new AtomicReference<>();
    try (TestHttpServer server =
        TestHttpServer.create(
            "/api/v1/lineage",
            exchange -> {
              body.set(
                  new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
              auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
              exchange.sendResponseHeaders(204, -1);
            })) {
      HttpLineageForwarder forwarder =
          new HttpLineageForwarder(
              config(
                  true,
                  Set.of("marquez"),
                  Map.of(
                      "marquez",
                      target(
                          server.uri(),
                          "api/v1/lineage",
                          FailureMode.FAIL_CLOSED,
                          auth(AuthType.API_KEY, "test-key")))));

      forwarder.forwardRawEvent(RAW_EVENT);

      assertThat(body.get()).isEqualTo(new String(RAW_EVENT, StandardCharsets.UTF_8));
      assertThat(auth.get()).isEqualTo("test-key");
    }
  }

  void failClosedTargetFailureFailsIngestPath() throws Exception {
    try (TestHttpServer server = failingServer()) {
      HttpLineageForwarder forwarder =
          new HttpLineageForwarder(
              config(
                  true,
                  Set.of("marquez"),
                  Map.of(
                      "marquez",
                      target(
                          server.uri(),
                          "api/v1/lineage",
                          FailureMode.FAIL_CLOSED,
                          auth(AuthType.NONE, null)))));

      assertThatThrownBy(() -> forwarder.forwardRawEvent(RAW_EVENT))
          .isInstanceOf(LineageForwardingException.class)
          .hasMessageContaining("HTTP 503");
    }
  }

  @Test
  void failOpenTargetFailureAllowsLocalPersistencePathToContinue() throws Exception {
    try (TestHttpServer server = failingServer()) {
      HttpLineageForwarder forwarder =
          new HttpLineageForwarder(
              config(
                  true,
                  Set.of("marquez"),
                  Map.of(
                      "marquez",
                      target(
                          server.uri(),
                          "api/v1/lineage",
                          FailureMode.FAIL_OPEN,
                          auth(AuthType.NONE, null)))));

      assertThatCode(() -> forwarder.forwardRawEvent(RAW_EVENT)).doesNotThrowAnyException();
    }
  }

  @Test
  void configuredTargetMustExist() {
    HttpLineageForwarder forwarder =
        new HttpLineageForwarder(config(true, Set.of("missing"), Map.of()));

    assertThatThrownBy(() -> forwarder.forwardRawEvent(RAW_EVENT))
        .isInstanceOf(LineageForwardingException.class)
        .hasMessageContaining("missing");
  }

  private static TestHttpServer failingServer() throws IOException {
    return TestHttpServer.create(
        "/api/v1/lineage",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(503, -1);
        });
  }

  private static LineageForwardingConfiguration config(
      boolean enabled, Set<String> targets, Map<String, TargetConfiguration> targetConfigurations) {
    return new TestConfiguration(enabled, Optional.ofNullable(targets), targetConfigurations);
  }

  private static TargetConfiguration target(URI url) {
    return target(url, "api/v1/lineage", FailureMode.FAIL_CLOSED, auth(AuthType.NONE, null));
  }

  private static TargetConfiguration target(
      URI url, String endpoint, FailureMode failureMode, AuthConfiguration auth) {
    return new TestTargetConfiguration(url, endpoint, Duration.ofSeconds(5), failureMode, auth);
  }

  private static AuthConfiguration auth(AuthType type, String apiKey) {
    return new TestAuthConfiguration(type, Optional.ofNullable(apiKey));
  }

  private static class TestConfiguration implements LineageForwardingConfiguration {
    private final boolean enabled;
    private final Optional<Set<String>> targets;
    private final Map<String, TargetConfiguration> targetConfigurations;

    private TestConfiguration(
        boolean enabled,
        Optional<Set<String>> targets,
        Map<String, TargetConfiguration> targetConfigurations) {
      this.enabled = enabled;
      this.targets = targets;
      this.targetConfigurations = targetConfigurations;
    }

    @Override
    public boolean enabled() {
      return enabled;
    }

    @Override
    public Optional<Set<String>> targets() {
      return targets;
    }

    @Override
    public Map<String, TargetConfiguration> targetConfigurations() {
      return targetConfigurations;
    }
  }

  private static class TestTargetConfiguration implements TargetConfiguration {
    private final URI url;
    private final String endpoint;
    private final Duration timeout;
    private final FailureMode failureMode;
    private final AuthConfiguration auth;

    private TestTargetConfiguration(
        URI url,
        String endpoint,
        Duration timeout,
        FailureMode failureMode,
        AuthConfiguration auth) {
      this.url = url;
      this.endpoint = endpoint;
      this.timeout = timeout;
      this.failureMode = failureMode;
      this.auth = auth;
    }

    @Override
    public URI url() {
      return url;
    }

    @Override
    public String endpoint() {
      return endpoint;
    }

    @Override
    public Duration timeout() {
      return timeout;
    }

    @Override
    public FailureMode failureMode() {
      return failureMode;
    }

    @Override
    public AuthConfiguration auth() {
      return auth;
    }
  }

  private static class TestAuthConfiguration implements AuthConfiguration {
    private final AuthType type;
    private final Optional<String> apiKey;

    private TestAuthConfiguration(AuthType type, Optional<String> apiKey) {
      this.type = type;
      this.apiKey = apiKey;
    }

    @Override
    public AuthType type() {
      return type;
    }

    @Override
    public Optional<String> apiKey() {
      return apiKey;
    }
  }

  private static class TestHttpServer implements AutoCloseable {
    private final HttpServer server;

    private TestHttpServer(HttpServer server) {
      this.server = server;
    }

    private static TestHttpServer create(String path, com.sun.net.httpserver.HttpHandler handler)
        throws IOException {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext(path, handler);
      server.start();
      return new TestHttpServer(server);
    }

    private URI uri() {
      return URI.create("http://localhost:" + server.getAddress().getPort());
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
