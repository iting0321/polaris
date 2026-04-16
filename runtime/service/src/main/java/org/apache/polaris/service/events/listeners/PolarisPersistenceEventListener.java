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

package org.apache.polaris.service.events.listeners;

import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.openlineage.IcebergOpenLineageMapper;
import org.apache.polaris.service.events.openlineage.OpenLineageConfiguration;
import org.apache.polaris.service.events.openlineage.OpenLineageCreateTracker;
import org.apache.polaris.service.events.openlineage.OpenLineageInputTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PolarisPersistenceEventListener implements PolarisEventListener {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PolarisPersistenceEventListener.class);

  /**
   * Injected by CDI when running inside the Quarkus container. May be {@code null} in plain unit
   * tests (where CDI is not active); in that case {@link #isOpenLineageEnabled()} defaults to
   * {@code true} and the producer URI falls back to a default value.
   */
  @Inject OpenLineageConfiguration openLineageConfig;

  @Inject OpenLineageInputTracker openLineageInputTracker;
  @Inject OpenLineageCreateTracker openLineageCreateTracker;

  // ---------- PolarisEventListener ----------

  @Override
  public void onEvent(PolarisEvent event) {
    switch (event.type()) {
      case AFTER_CREATE_TABLE -> handleAfterCreateTable(event);
      case AFTER_UPDATE_TABLE -> handleAfterUpdateTable(event);
      case AFTER_DROP_TABLE -> handleAfterDropTable(event);
      case AFTER_LOAD_TABLE -> trackLoadedTable(event);
      case AFTER_CREATE_CATALOG -> handleAfterCreateCatalog(event);
      default -> {
        // Other events not handled by this listener
      }
    }
  }

  /** Returns {@code true} when OpenLineage facets should be generated and stored. */
  protected boolean isOpenLineageEnabled() {
    // null → CDI not active (plain unit test) → enable by default
    return openLineageConfig == null || openLineageConfig.enabled();
  }

  /** Returns the producer URI to embed in every OpenLineage facet. */
  protected URI openLineageProducerUri() {
    if (openLineageConfig == null) {
      return URI.create("https://github.com/apache/polaris");
    }
    return URI.create(openLineageConfig.producer());
  }

  private void handleAfterCreateTable(PolarisEvent event) {
    LoadTableResponse loadTableResponse =
        event.attributes().getRequired(EventAttributes.LOAD_TABLE_RESPONSE);
    TableMetadata tableMetadata = loadTableResponse.tableMetadata();
    String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalogName,
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE,
            tableIdentifier.toString());

    var additionalParameters =
        ImmutableMap.<String, String>builder()
            .put("table-uuid", tableMetadata.uuid())
            .put("metadata", TableMetadataParser.toJson(tableMetadata));

    if (isOpenLineageEnabled()) {
      if (openLineageCreateTracker != null) {
        openLineageCreateTracker.record(tableIdentifier, event.metadata().requestId().orElse(null));
      }
      try {
        additionalParameters.put(
            "openlineage",
            IcebergOpenLineageMapper.toTableRunEventJson(
                openLineageProducerUri(),
                catalogName,
                event.metadata().realmId(),
                PolarisEventType.AFTER_CREATE_TABLE,
                event.metadata().requestId().orElse(null),
                event.metadata().timestamp(),
                resolveCreateInputs(tableIdentifier, tableMetadata),
                tableIdentifier,
                tableMetadata));
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to serialize OpenLineage payload for table {}", tableIdentifier, e);
      }
    }

    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private void handleAfterUpdateTable(PolarisEvent event) {
    String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
    TableIdentifier tableIdentifier = resolveTableIdentifier(event);
    TableMetadata tableMetadata = event.attributes().getRequired(EventAttributes.TABLE_METADATA);
    PolarisEventType effectiveEventType =
        shouldTreatUpdateAsCreate(
                event, tableIdentifier, tableMetadata, event.metadata().requestId().orElse(null))
            ? PolarisEventType.AFTER_CREATE_TABLE
            : PolarisEventType.AFTER_UPDATE_TABLE;

    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalogName,
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE,
            tableIdentifier.toString());

    var additionalParameters = ImmutableMap.<String, String>builder();
    if (tableMetadata != null) {
      additionalParameters.put("table-uuid", tableMetadata.uuid());
      additionalParameters.put("metadata", TableMetadataParser.toJson(tableMetadata));

      if (isOpenLineageEnabled()) {
        try {
          additionalParameters.put(
              "openlineage",
              IcebergOpenLineageMapper.toTableRunEventJson(
                  openLineageProducerUri(),
                  catalogName,
                  event.metadata().realmId(),
                  effectiveEventType,
                  event.metadata().requestId().orElse(null),
                  event.metadata().timestamp(),
                  effectiveEventType == PolarisEventType.AFTER_CREATE_TABLE
                      ? resolveCreateInputs(tableIdentifier, tableMetadata)
                      : List.of(),
                  tableIdentifier,
                  tableMetadata));
        } catch (RuntimeException e) {
          LOGGER.warn("Failed to serialize OpenLineage payload for table {}", tableIdentifier, e);
        }
      }
    }

    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private void handleAfterDropTable(PolarisEvent event) {
    String catalogName = event.attributes().getRequired(EventAttributes.CATALOG_NAME);
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalogName,
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.TABLE,
            tableIdentifier.toString());

    var additionalParameters = ImmutableMap.<String, String>builder();
    if (isOpenLineageEnabled()) {
      try {
        additionalParameters.put(
            "openlineage",
            IcebergOpenLineageMapper.toDropRunEventJson(
                openLineageProducerUri(),
                catalogName,
                event.metadata().realmId(),
                event.metadata().requestId().orElse(null),
                event.metadata().timestamp(),
                tableIdentifier));
      } catch (RuntimeException e) {
        LOGGER.warn("Failed to serialize OpenLineage payload for table {}", tableIdentifier, e);
      }
    }

    additionalParameters.putAll(event.metadata().openTelemetryContext());
    polarisEvent.setAdditionalProperties(additionalParameters.build());
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private void handleAfterCreateCatalog(PolarisEvent event) {
    Catalog catalog = event.attributes().getRequired(EventAttributes.CATALOG);
    org.apache.polaris.core.entity.PolarisEvent polarisEvent =
        new org.apache.polaris.core.entity.PolarisEvent(
            catalog.getName(),
            event.metadata().eventId().toString(),
            event.metadata().requestId().orElse(null),
            event.type().name(),
            event.metadata().timestamp().toEpochMilli(),
            event.metadata().user().map(PolarisPrincipal::getName).orElse(null),
            org.apache.polaris.core.entity.PolarisEvent.ResourceType.CATALOG,
            catalog.getName());
    Map<String, String> openTelemetryContext = event.metadata().openTelemetryContext();
    if (!openTelemetryContext.isEmpty()) {
      polarisEvent.setAdditionalProperties(openTelemetryContext);
    }
    processEvent(event.metadata().realmId(), polarisEvent);
  }

  private void trackLoadedTable(PolarisEvent event) {
    if (openLineageInputTracker == null) {
      return;
    }
    LoadTableResponse loadTableResponse =
        event.attributes().get(EventAttributes.LOAD_TABLE_RESPONSE).orElse(null);
    if (loadTableResponse == null || loadTableResponse.tableMetadata() == null) {
      return;
    }
    Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
    String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);
    openLineageInputTracker.record(
        TableIdentifier.of(namespace, tableName), loadTableResponse.tableMetadata());
  }

  private List<IcebergOpenLineageMapper.LineageDataset> resolveCreateInputs(
      TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    if (openLineageInputTracker == null) {
      return List.of();
    }
    return openLineageInputTracker.inputsFor(tableIdentifier, tableMetadata);
  }

  private boolean shouldTreatUpdateAsCreate(
      PolarisEvent event,
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      String requestId) {
    if (tableMetadata == null || tableMetadata.currentSnapshot() == null) {
      return false;
    }
    if (tableMetadata.snapshots().size() != 1) {
      return false;
    }
    UpdateTableRequest updateTableRequest =
        event.attributes().get(EventAttributes.UPDATE_TABLE_REQUEST).orElse(null);
    if (isCreateUpdateRequest(updateTableRequest)) {
      return true;
    }
    if (openLineageCreateTracker == null) {
      return false;
    }
    return openLineageCreateTracker.consumeIfMatches(tableIdentifier, requestId);
  }

  private static boolean isCreateUpdateRequest(UpdateTableRequest updateTableRequest) {
    return updateTableRequest != null
        && updateTableRequest.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
  }

  private static TableIdentifier resolveTableIdentifier(PolarisEvent event) {
    return event
        .attributes()
        .get(EventAttributes.TABLE_IDENTIFIER)
        .orElseGet(
            () -> {
              Namespace namespace = event.attributes().getRequired(EventAttributes.NAMESPACE);
              String tableName = event.attributes().getRequired(EventAttributes.TABLE_NAME);
              return TableIdentifier.of(namespace, tableName);
            });
  }

  protected abstract void processEvent(
      String realmId, org.apache.polaris.core.entity.PolarisEvent event);
}
