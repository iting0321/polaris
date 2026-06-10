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

import io.openlineage.server.OpenLineage;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageIngestRequest;
import org.apache.polaris.core.lineage.LineageService;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.apache.polaris.service.lineage.api.PolarisOpenLineageApiService;

/**
 * Dataset-level implementation of the OpenLineage ingest endpoint.
 *
 * <p>Run events are collapsed from OpenLineage's input-dataset -> job -> output-dataset model into
 * direct input-dataset -> output-dataset edges. Job events and dataset events are accepted for
 * OpenLineage compatibility, but do not contain both inputs and outputs and therefore do not
 * persist dataset graph changes.
 */
@RequestScoped
public class OpenLineageAdapter implements PolarisOpenLineageApiService {

  private static final String UNRESOLVED_CATALOG = "";

  private final LineageService lineageService;

  @Inject
  public OpenLineageAdapter(LineageService lineageService) {
    this.lineageService = lineageService;
  }

  @Override
  public Response sendLineageEvent(
      PolarisLineageEvent event, RealmContext realmContext, SecurityContext securityContext) {
    if (event.event() instanceof OpenLineage.RunEvent runEvent) {
      List<LineageDataset> inputs = datasets(runEvent.getInputs());
      List<LineageDataset> outputs = datasets(runEvent.getOutputs());
      if (!inputs.isEmpty() && !outputs.isEmpty()) {
        lineageService.ingest(
            new LineageIngestRequest(
                datasets(inputs, outputs),
                edges(inputs, outputs),
                List.of(),
                eventTime(runEvent)));
      }
    }
    return Response.status(Response.Status.CREATED).build();
  }

  private static List<LineageDataset> datasets(
      List<? extends OpenLineage.Dataset> openLineageDatasets) {
    if (openLineageDatasets == null) {
      return List.of();
    }

    Set<LineageDataset> datasets = new LinkedHashSet<>();
    for (OpenLineage.Dataset dataset : openLineageDatasets) {
      String namespace = text(dataset.getNamespace());
      String name = text(dataset.getName());
      if (namespace != null && name != null) {
        datasets.add(new LineageDataset(UNRESOLVED_CATALOG, namespace, name));
      }
    }
    return new ArrayList<>(datasets);
  }

  private static List<LineageDataset> datasets(
      List<LineageDataset> inputs, List<LineageDataset> outputs) {
    Set<LineageDataset> datasets = new LinkedHashSet<>();
    datasets.addAll(inputs);
    datasets.addAll(outputs);
    return new ArrayList<>(datasets);
  }

  private static List<LineageEdge> edges(
      List<LineageDataset> inputs, List<LineageDataset> outputs) {
    List<LineageEdge> edges = new ArrayList<>();
    for (LineageDataset input : inputs) {
      for (LineageDataset output : outputs) {
        edges.add(new LineageEdge(input, output));
      }
    }
    return edges;
  }

  private static String text(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }

  private static Optional<Instant> eventTime(OpenLineage.RunEvent runEvent) {
    return runEvent.getEventTime() == null
        ? Optional.empty()
        : Optional.of(runEvent.getEventTime().toInstant());
  }
}
