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
package org.apache.polaris.service.events.listeners.http;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.events.listeners.PolarisPersistenceEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("persistence-http-openlineage")
public class HttpOpenLineageEventListener extends PolarisPersistenceEventListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpOpenLineageEventListener.class);

  private final HttpClient httpClient;
  private final URI marquezUri;
  private final HttpOpenLineageEventListenerConfiguration configuration;

  @Inject
  public HttpOpenLineageEventListener(HttpOpenLineageEventListenerConfiguration configuration) {
    this.configuration = configuration;
    this.httpClient =
        HttpClient.newBuilder().connectTimeout(configuration.connectTimeout()).build();
    this.marquezUri = URI.create(configuration.endpoint());
  }

  @Override
  protected void processEvent(String realmId, PolarisEvent event) {
    Map<String, String> properties = event.getAdditionalPropertiesAsMap();
    if (properties == null) {
      return;
    }

    String openLineageJson = properties.get("openlineage");
    if (openLineageJson == null || openLineageJson.isBlank()) {
      return;
    }

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(marquezUri)
            .timeout(configuration.requestTimeout())
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(openLineageJson))
            .build();

    httpClient
        .sendAsync(request, HttpResponse.BodyHandlers.discarding())
        .thenAccept(
            response -> {
              if (response.statusCode() == 201) {
                LOGGER.debug(
                    "Successfully sent OpenLineage event to Marquez for realm '{}'", realmId);
              } else {
                LOGGER.warn(
                    "Marquez returned unexpected status {} for realm '{}'",
                    response.statusCode(),
                    realmId);
              }
            })
        .exceptionally(
            throwable -> {
              LOGGER.error(
                  "Failed to send OpenLineage event to Marquez for realm '{}'", realmId, throwable);
              return null;
            });
  }
}
