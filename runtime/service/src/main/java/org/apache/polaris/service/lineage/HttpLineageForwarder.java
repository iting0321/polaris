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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.AuthConfiguration;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.AuthType;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.FailureMode;
import org.apache.polaris.service.lineage.LineageForwardingConfiguration.TargetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class HttpLineageForwarder implements LineageForwarder {
  private static final Logger LOGGER = LoggerFactory.getLogger(HttpLineageForwarder.class);

  private final LineageForwardingConfiguration configuration;
  private final HttpClient httpClient;

  @Inject
  public HttpLineageForwarder(LineageForwardingConfiguration configuration) {
    this(configuration, HttpClient.newHttpClient());
  }

  HttpLineageForwarder(LineageForwardingConfiguration configuration, HttpClient httpClient) {
    this.configuration = configuration;
    this.httpClient = httpClient;
  }

  @Override
  public void forwardRawEvent(byte[] rawEvent) {
    if (!configuration.enabled()) {
      return;
    }

    for (String targetName : targetNames()) {
      TargetConfiguration target = target(targetName);
      try {
        forwardToTarget(targetName, target, rawEvent);
      } catch (RuntimeException e) {
        if (target.failureMode() == FailureMode.FAIL_OPEN) {
          LOGGER.warn("OpenLineage forwarding to target '{}' failed; continuing", targetName, e);
        } else {
          throw e;
        }
      }
    }
  }

  private Set<String> targetNames() {
    Optional<Set<String>> configuredTargets = configuration.targets();
    if (configuredTargets.isPresent()) {
      return configuredTargets.get();
    }
    return configuration.targetConfigurations().keySet();
  }

  private TargetConfiguration target(String targetName) {
    Map<String, TargetConfiguration> targets = configuration.targetConfigurations();
    TargetConfiguration target = targets.get(targetName);
    if (target == null) {
      throw new LineageForwardingException(
          "OpenLineage forwarding target '" + targetName + "' is not configured");
    }
    return target;
  }

  private void forwardToTarget(String targetName, TargetConfiguration target, byte[] rawEvent) {
    HttpRequest request =
        withAuth(
                HttpRequest.newBuilder(targetUri(target))
                    .timeout(target.timeout())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(rawEvent)),
                target.auth())
            .build();

    HttpResponse<Void> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
    } catch (IOException e) {
      throw new LineageForwardingException(
          "OpenLineage forwarding to target '" + targetName + "' failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new LineageForwardingException(
          "OpenLineage forwarding to target '" + targetName + "' was interrupted", e);
    }

    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new LineageForwardingException(
          "OpenLineage forwarding to target '"
              + targetName
              + "' returned HTTP "
              + response.statusCode());
    }
  }

  private static URI targetUri(TargetConfiguration target) {
    String base = target.url().toString();
    String endpoint = target.endpoint();
    if (endpoint.startsWith("/")) {
      endpoint = endpoint.substring(1);
    }
    if (!base.endsWith("/")) {
      base = base + "/";
    }
    return URI.create(base + endpoint);
  }

  private static HttpRequest.Builder withAuth(
      HttpRequest.Builder requestBuilder, AuthConfiguration auth) {
    AuthType type = auth.type();
    switch (type) {
      case NONE:
        return requestBuilder;
      case API_KEY:
        // TODO: Add secret-reference based downstream auth modes before supporting bearer, basic,
        // OAuth, or custom API-key headers.
        return requestBuilder.header("Authorization", required(auth.apiKey(), "api-key"));
      default:
        throw new LineageForwardingException(
            "Unsupported OpenLineage forwarding auth type: " + type);
    }
  }

  private static String required(Optional<String> value, String propertyName) {
    return value.orElseThrow(
        () ->
            new LineageForwardingException(
                "OpenLineage forwarding auth property '" + propertyName + "' is required"));
  }
}
