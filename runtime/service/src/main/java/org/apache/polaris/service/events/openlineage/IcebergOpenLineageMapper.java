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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.polaris.service.events.PolarisEventType;

/**
 * Maps Iceberg table mutations to OpenLineage run events.
 *
 * <p>Marquez's UI is job-centric. Emitting synthetic Polaris jobs with dataset inputs/outputs makes
 * table create/update/drop activity visible in the graph, while preserving dataset facets such as
 * schema, datasource, lifecycle state, and version.
 */
public final class IcebergOpenLineageMapper {
  public record LineageDataset(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {}

  private static final ObjectMapper MAPPER = OpenLineageClientUtils.newObjectMapper();

  private IcebergOpenLineageMapper() {}

  public static String toTableRunEventJson(
      URI producerUri,
      String catalogName,
      String realmId,
      PolarisEventType eventType,
      String requestId,
      Instant eventTime,
      List<LineageDataset> inputDatasets,
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata) {
    OpenLineage ol = new OpenLineage(producerUri);
    OpenLineage.DatasetFacets datasetFacets =
        buildDatasetFacets(ol, eventType, tableIdentifier, tableMetadata);
    Snapshot snapshot = tableMetadata.currentSnapshot();

    List<OpenLineage.InputDataset> inputs =
        eventType == PolarisEventType.AFTER_UPDATE_TABLE
            ? List.of(buildInputDataset(ol, tableIdentifier, datasetFacets))
            : buildInputDatasets(ol, inputDatasets);

    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(toZonedDateTime(eventTime))
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(buildRun(ol, requestId, eventTime))
            .job(buildJob(ol, catalogName, realmId, eventType, tableIdentifier))
            .inputs(inputs)
            .outputs(List.of(buildOutputDataset(ol, tableIdentifier, datasetFacets, snapshot)))
            .build();

    return serialize(event);
  }

  public static String toDropRunEventJson(
      URI producerUri,
      String catalogName,
      String realmId,
      String requestId,
      Instant eventTime,
      TableIdentifier tableIdentifier) {
    OpenLineage ol = new OpenLineage(producerUri);
    OpenLineage.DatasetFacets facets =
        ol.newDatasetFacetsBuilder()
            .dataSource(buildDatasourceFacet(ol, tableIdentifier, null))
            .lifecycleStateChange(
                ol.newLifecycleStateChangeDatasetFacetBuilder()
                    .lifecycleStateChange(
                        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP)
                    .build())
            .build();

    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(toZonedDateTime(eventTime))
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .run(buildRun(ol, requestId, eventTime))
            .job(
                buildJob(
                    ol, catalogName, realmId, PolarisEventType.AFTER_DROP_TABLE, tableIdentifier))
            .inputs(List.of(buildInputDataset(ol, tableIdentifier, facets)))
            .outputs(List.of())
            .build();

    return serialize(event);
  }

  private static OpenLineage.Run buildRun(OpenLineage ol, String requestId, Instant eventTime) {
    return ol.newRunBuilder()
        .runId(stableRunId(requestId, eventTime))
        .facets(
            ol.newRunFacetsBuilder()
                .processing_engine(
                    ol.newProcessingEngineRunFacetBuilder()
                        .name("polaris")
                        .openlineageAdapterVersion("1.0")
                        .build())
                .build())
        .build();
  }

  private static UUID stableRunId(String requestId, Instant eventTime) {
    String source = requestId == null || requestId.isBlank() ? eventTime.toString() : requestId;
    return UUID.nameUUIDFromBytes(source.getBytes(StandardCharsets.UTF_8));
  }

  private static OpenLineage.Job buildJob(
      OpenLineage ol,
      String catalogName,
      String realmId,
      PolarisEventType eventType,
      TableIdentifier tableIdentifier) {
    return ol.newJobBuilder()
        .namespace("polaris." + realmId + "." + catalogName)
        .name(eventType.name().toLowerCase(Locale.ROOT) + ":" + tableIdentifier)
        .facets(
            ol.newJobFacetsBuilder()
                .jobType(
                    ol.newJobTypeJobFacetBuilder()
                        .processingType("BATCH")
                        .integration("POLARIS")
                        .jobType("COMMAND")
                        .build())
                .documentation(
                    ol.newDocumentationJobFacetBuilder()
                        .description(
                            "Synthetic Polaris lineage job for "
                                + eventType.name()
                                + " on "
                                + tableIdentifier)
                        .build())
                .build())
        .build();
  }

  private static List<OpenLineage.InputDataset> buildInputDatasets(
      OpenLineage ol, List<LineageDataset> inputDatasets) {
    if (inputDatasets == null || inputDatasets.isEmpty()) {
      return List.of();
    }
    return inputDatasets.stream()
        .map(
            dataset ->
                buildInputDataset(
                    ol,
                    dataset.tableIdentifier(),
                    buildDatasetFacets(
                        ol,
                        PolarisEventType.AFTER_LOAD_TABLE,
                        dataset.tableIdentifier(),
                        dataset.tableMetadata())))
        .collect(Collectors.toList());
  }

  private static OpenLineage.DatasetFacets buildDatasetFacets(
      OpenLineage ol,
      PolarisEventType eventType,
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata) {
    OpenLineage.DatasetFacetsBuilder facetsBuilder =
        ol.newDatasetFacetsBuilder()
            .schema(buildSchemaFacet(ol, tableMetadata.schema()))
            .dataSource(buildDatasourceFacet(ol, tableIdentifier, tableMetadata));

    Snapshot snapshot = tableMetadata.currentSnapshot();
    if (snapshot != null) {
      addSnapshotFacets(ol, facetsBuilder, eventType, tableMetadata, snapshot);
    }
    return facetsBuilder.build();
  }

  private static OpenLineage.InputDataset buildInputDataset(
      OpenLineage ol, TableIdentifier tableIdentifier, OpenLineage.DatasetFacets facets) {
    return ol.newInputDatasetBuilder()
        .namespace(tableIdentifier.namespace().toString())
        .name(tableIdentifier.name())
        .facets(facets)
        .build();
  }

  private static OpenLineage.OutputDataset buildOutputDataset(
      OpenLineage ol,
      TableIdentifier tableIdentifier,
      OpenLineage.DatasetFacets facets,
      Snapshot snapshot) {
    OpenLineage.OutputDatasetBuilder builder =
        ol.newOutputDatasetBuilder()
            .namespace(tableIdentifier.namespace().toString())
            .name(tableIdentifier.name())
            .facets(facets);

    if (snapshot != null) {
      builder.outputFacets(
          ol.newOutputDatasetOutputFacetsBuilder()
              .outputStatistics(
                  ol.newOutputStatisticsOutputDatasetFacetBuilder()
                      .rowCount(summaryLong(snapshot, "total-records"))
                      .size(summaryLong(snapshot, "total-file-size"))
                      .fileCount(summaryLong(snapshot, "added-data-files"))
                      .build())
              .build());
    }

    return builder.build();
  }

  private static OpenLineage.DatasourceDatasetFacet buildDatasourceFacet(
      OpenLineage ol, TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    URI uri =
        tableMetadata != null && tableMetadata.location() != null
            ? URI.create(tableMetadata.location())
            : URI.create(
                "urn:polaris:" + tableIdentifier.namespace() + "." + tableIdentifier.name());

    return ol.newDatasourceDatasetFacetBuilder()
        .name(uri.getScheme() == null ? "polaris" : uri.getScheme())
        .uri(uri)
        .build();
  }

  private static OpenLineage.SchemaDatasetFacet buildSchemaFacet(OpenLineage ol, Schema schema) {
    List<OpenLineage.SchemaDatasetFacetFields> fields =
        schema.columns().stream().map(col -> buildField(ol, col)).collect(Collectors.toList());
    return ol.newSchemaDatasetFacetBuilder().fields(fields).build();
  }

  private static OpenLineage.SchemaDatasetFacetFields buildField(
      OpenLineage ol, Types.NestedField col) {
    OpenLineage.SchemaDatasetFacetFieldsBuilder builder =
        ol.newSchemaDatasetFacetFieldsBuilder().name(col.name()).type(col.type().toString());
    if (col.doc() != null) {
      builder.description(col.doc());
    }
    return builder.build();
  }

  private static void addSnapshotFacets(
      OpenLineage ol,
      OpenLineage.DatasetFacetsBuilder facetsBuilder,
      PolarisEventType eventType,
      TableMetadata metadata,
      Snapshot snapshot) {
    facetsBuilder.version(
        ol.newDatasetVersionDatasetFacetBuilder()
            .datasetVersion(String.valueOf(snapshot.snapshotId()))
            .build());

    if (eventType != PolarisEventType.AFTER_LOAD_TABLE) {
      facetsBuilder.lifecycleStateChange(
          ol.newLifecycleStateChangeDatasetFacetBuilder()
              .lifecycleStateChange(mapLifecycleState(eventType, metadata, snapshot))
              .build());
    }
  }

  private static OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange
      mapLifecycleState(PolarisEventType eventType, TableMetadata metadata, Snapshot snapshot) {
    // Lifecycle mapping intentionally follows user-visible table semantics:
    // - create -> CREATE
    // - append/schema evolution -> ALTER
    // - overwrite/replace -> OVERWRITE
    // - drop is handled by toDropRunEventJson() as DROP
    if (eventType == PolarisEventType.AFTER_CREATE_TABLE && metadata.snapshots().size() == 1) {
      return OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE;
    }

    String operation = snapshot.operation();
    if (DataOperations.OVERWRITE.equals(operation) || DataOperations.REPLACE.equals(operation)) {
      return OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE;
    }

    return OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER;
  }

  private static Long summaryLong(Snapshot snapshot, String key) {
    if (snapshot.summary() == null) {
      return null;
    }
    String value = snapshot.summary().get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static ZonedDateTime toZonedDateTime(Instant eventTime) {
    return ZonedDateTime.ofInstant(eventTime, ZoneOffset.UTC);
  }

  private static String serialize(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize OpenLineage event", e);
    }
  }
}
