/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.service.events.openlineage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/**
 * Maps Iceberg table metadata to OpenLineage dataset objects using the OpenLineage Java SDK.
 *
 * <p>Emits standalone {@code DatasetEvent}s (Static Datasets) appropriate for a Catalog. *
 *
 * <p>Facets emitted per event type:
 *
 * <ul>
 *   <li><b>CREATE / UPDATE</b> — dataset {@code schema}, and when a snapshot exists: {@code
 *       lifecycleStateChange} and {@code version}.
 *   <li><b>DROP</b> — dataset {@code lifecycleStateChange} (value: {@code DROP}).
 * </ul>
 */
public final class IcebergOpenLineageMapper {

  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();

  private IcebergOpenLineageMapper() {}

  // ---------- public API ----------

  /**
   * Generates a complete OpenLineage DatasetEvent (including the Event envelope) representing the
   * current static state of the Iceberg table.
   */
  public static String toDatasetEventJson(
      URI producerUri, TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    OpenLineage ol = new OpenLineage(producerUri);

    // 1. Build Core Facets
    OpenLineage.DatasetFacetsBuilder facetsBuilder =
        ol.newDatasetFacetsBuilder().schema(buildSchemaFacet(ol, tableMetadata.schema()));

    Snapshot snapshot = tableMetadata.currentSnapshot();
    if (snapshot != null) {
      addSnapshotFacets(ol, facetsBuilder, tableMetadata, snapshot);
    }

    // 2. Build StaticDataset (DatasetEvent strictly requires a StaticDataset)
    OpenLineage.StaticDataset dataset =
        ol.newStaticDatasetBuilder()
            .namespace(tableIdentifier.namespace().toString())
            .name(tableIdentifier.name())
            .facets(facetsBuilder.build())
            .build();

    // 3. Wrap into a complete DatasetEvent
    OpenLineage.DatasetEvent event =
        ol.newDatasetEventBuilder().eventTime(ZonedDateTime.now()).dataset(dataset).build();

    return serialize(event);
  }

  /** Generates a complete OpenLineage DatasetEvent representing a DROP operation. */
  public static String toDropDatasetEventJson(URI producerUri, TableIdentifier tableIdentifier) {
    OpenLineage ol = new OpenLineage(producerUri);

    OpenLineage.DatasetFacets facets =
        ol.newDatasetFacetsBuilder()
            .lifecycleStateChange(
                ol.newLifecycleStateChangeDatasetFacetBuilder()
                    .lifecycleStateChange(
                        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP)
                    .build())
            .build();

    OpenLineage.StaticDataset dataset =
        ol.newStaticDatasetBuilder()
            .namespace(tableIdentifier.namespace().toString())
            .name(tableIdentifier.name())
            .facets(facets)
            .build();

    OpenLineage.DatasetEvent event =
        ol.newDatasetEventBuilder().eventTime(ZonedDateTime.now()).dataset(dataset).build();

    return serialize(event);
  }

  // ---------- facet helpers ----------

  private static OpenLineage.SchemaDatasetFacet buildSchemaFacet(OpenLineage ol, Schema schema) {
    List<OpenLineage.SchemaDatasetFacetFields> fields =
        schema.columns().stream().map(col -> buildField(ol, col)).collect(Collectors.toList());
    return ol.newSchemaDatasetFacetBuilder().fields(fields).build();
  }

  private static OpenLineage.SchemaDatasetFacetFields buildField(
      OpenLineage ol, Types.NestedField col) {
    OpenLineage.SchemaDatasetFacetFieldsBuilder b =
        ol.newSchemaDatasetFacetFieldsBuilder().name(col.name()).type(col.type().toString());
    if (col.doc() != null) {
      b.description(col.doc());
    }
    return b.build();
  }

  private static void addSnapshotFacets(
      OpenLineage ol,
      OpenLineage.DatasetFacetsBuilder facetsBuilder,
      TableMetadata metadata,
      Snapshot snapshot) {

    facetsBuilder
        .version(
            ol.newDatasetVersionDatasetFacetBuilder()
                .datasetVersion(String.valueOf(snapshot.snapshotId()))
                .build())
        .lifecycleStateChange(
            ol.newLifecycleStateChangeDatasetFacetBuilder()
                .lifecycleStateChange(mapLifecycleState(metadata, snapshot))
                .build());
  }

  private static OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
      mapLifecycleState(TableMetadata metadata, Snapshot snapshot) {

    // If this is the very first snapshot of the table, it represents a CREATE event.
    if (metadata.snapshots().size() == 1) {
      return OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
    }

    String operation = snapshot.operation();
    if (DataOperations.OVERWRITE.equals(operation) || DataOperations.REPLACE.equals(operation)) {
      return OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
    }

    // All other operations (e.g., Append, Delete) are categorized as table alterations (ALTER).
    return OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER;
  }

  private static String serialize(Object obj) {
    try {
      com.fasterxml.jackson.databind.node.ObjectNode node =
          (com.fasterxml.jackson.databind.node.ObjectNode) MAPPER.valueToTree(obj);
      node.put("eventType", "DATASET");
      return MAPPER.writeValueAsString(node);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize OpenLineage event", e);
    }
  }
}
