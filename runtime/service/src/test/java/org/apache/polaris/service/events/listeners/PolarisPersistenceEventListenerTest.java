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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.service.events.EventAttributeMap;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.ImmutablePolarisEventMetadata;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.Test;

class PolarisPersistenceEventListenerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // ---------- AFTER_UPDATE_TABLE ----------

  @Test
  void afterUpdateTablePersistsOpenLineagePayload() throws Exception {
    TableMetadata tableMetadata = buildTableMetadata();

    CaptureListener listener = new CaptureListener();
    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_TABLE,
            ImmutablePolarisEventMetadata.builder()
                .realmId("test-realm")
                .requestId("req-123")
                .openTelemetryContext(Map.of("otel.trace_id", "trace-123"))
                .build(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "test-catalog")
                .put(
                    EventAttributes.TABLE_IDENTIFIER,
                    TableIdentifier.of("db_sales", "daily_orders"))
                .put(EventAttributes.TABLE_METADATA, tableMetadata)));

    assertThat(listener.realmId).isEqualTo("test-realm");
    assertThat(listener.persistedEvent).isNotNull();
    assertThat(listener.persistedEvent.getCatalogId()).isEqualTo("test-catalog");
    assertThat(listener.persistedEvent.getEventType()).isEqualTo("AFTER_UPDATE_TABLE");
    assertThat(listener.persistedEvent.getResourceIdentifier()).isEqualTo("db_sales.daily_orders");
    assertThat(listener.persistedEvent.getAdditionalPropertiesAsMap())
        .containsEntry("table-uuid", "test-uuid")
        .containsKey("metadata")
        .containsEntry("otel.trace_id", "trace-123")
        .containsKey("openlineage");

    var openLineage =
        OBJECT_MAPPER.readTree(
            listener.persistedEvent.getAdditionalPropertiesAsMap().get("openlineage"));
    assertThat(openLineage.at("/dataset/namespace").asText()).isEqualTo("db_sales");
    assertThat(openLineage.at("/dataset/name").asText()).isEqualTo("daily_orders");
    assertThat(openLineage.at("/dataset/facets/schema/fields/0/name").asText()).isEqualTo("id");
    assertThat(openLineage.at("/dataset/facets/schema/fields/0/type").asText()).isEqualTo("int");
    assertThat(openLineage.at("/dataset/facets/schema/fields/0/description").asText())
        .isEqualTo("primary key");
    assertThat(openLineage.at("/dataset/facets/version").isMissingNode()).isTrue();
    assertThat(openLineage.at("/dataset/facets/lifecycleStateChange").isMissingNode()).isTrue();
  }

  @Test
  void afterUpdateTableSkipsOpenLineageWhenDisabled() {
    TableMetadata tableMetadata = buildTableMetadata();

    DisabledOpenLineageListener listener = new DisabledOpenLineageListener();
    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_UPDATE_TABLE,
            ImmutablePolarisEventMetadata.builder().realmId("r").build(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "cat")
                .put(EventAttributes.TABLE_IDENTIFIER, TableIdentifier.of("ns", "tbl"))
                .put(EventAttributes.TABLE_METADATA, tableMetadata)));

    assertThat(listener.persistedEvent.getAdditionalPropertiesAsMap())
        .containsKey("table-uuid")
        .doesNotContainKey("openlineage");
  }

  // ---------- AFTER_CREATE_TABLE ----------

  @Test
  void afterCreateTablePersistsOpenLineagePayload() throws Exception {
    TableMetadata tableMetadata = buildTableMetadata();
    LoadTableResponse loadTableResponse =
        LoadTableResponse.builder().withTableMetadata(tableMetadata).build();

    CaptureListener listener = new CaptureListener();
    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_CREATE_TABLE,
            ImmutablePolarisEventMetadata.builder()
                .realmId("test-realm")
                .requestId("req-456")
                .build(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "test-catalog")
                .put(EventAttributes.NAMESPACE, Namespace.of("db_sales"))
                .put(EventAttributes.TABLE_NAME, "new_table")
                .put(EventAttributes.LOAD_TABLE_RESPONSE, loadTableResponse)));

    assertThat(listener.persistedEvent.getAdditionalPropertiesAsMap())
        .containsEntry("table-uuid", "test-uuid")
        .containsKey("metadata")
        .containsKey("openlineage");

    var openLineage =
        OBJECT_MAPPER.readTree(
            listener.persistedEvent.getAdditionalPropertiesAsMap().get("openlineage"));
    assertThat(openLineage.at("/dataset/namespace").asText()).isEqualTo("db_sales");
    assertThat(openLineage.at("/dataset/name").asText()).isEqualTo("new_table");
    assertThat(openLineage.at("/dataset/facets/schema/fields/0/name").asText()).isEqualTo("id");
    // No snapshot yet on a freshly created table
    assertThat(openLineage.at("/dataset/facets/version").isMissingNode()).isTrue();
    assertThat(openLineage.at("/dataset/facets/lifecycleStateChange").isMissingNode()).isTrue();
  }

  // ---------- AFTER_DROP_TABLE ----------

  @Test
  void afterDropTablePersistsOpenLineageDropPayload() throws Exception {
    CaptureListener listener = new CaptureListener();
    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DROP_TABLE,
            ImmutablePolarisEventMetadata.builder()
                .realmId("test-realm")
                .requestId("req-789")
                .build(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "test-catalog")
                .put(EventAttributes.NAMESPACE, Namespace.of("db_sales"))
                .put(EventAttributes.TABLE_NAME, "daily_orders")));

    assertThat(listener.realmId).isEqualTo("test-realm");
    assertThat(listener.persistedEvent.getCatalogId()).isEqualTo("test-catalog");
    assertThat(listener.persistedEvent.getEventType()).isEqualTo("AFTER_DROP_TABLE");
    assertThat(listener.persistedEvent.getResourceIdentifier()).isEqualTo("db_sales.daily_orders");
    assertThat(listener.persistedEvent.getAdditionalPropertiesAsMap()).containsKey("openlineage");

    var openLineage =
        OBJECT_MAPPER.readTree(
            listener.persistedEvent.getAdditionalPropertiesAsMap().get("openlineage"));
    assertThat(openLineage.at("/dataset/namespace").asText()).isEqualTo("db_sales");
    assertThat(openLineage.at("/dataset/name").asText()).isEqualTo("daily_orders");
    assertThat(openLineage.at("/dataset/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("DROP");
    assertThat(openLineage.at("/dataset/facets/schema").isMissingNode()).isTrue();
  }

  @Test
  void afterDropTableSkipsOpenLineageWhenDisabled() {
    DisabledOpenLineageListener listener = new DisabledOpenLineageListener();
    listener.onEvent(
        new PolarisEvent(
            PolarisEventType.AFTER_DROP_TABLE,
            ImmutablePolarisEventMetadata.builder().realmId("r").build(),
            new EventAttributeMap()
                .put(EventAttributes.CATALOG_NAME, "cat")
                .put(EventAttributes.NAMESPACE, Namespace.of("ns"))
                .put(EventAttributes.TABLE_NAME, "tbl")));

    assertThat(listener.persistedEvent.getAdditionalPropertiesAsMap())
        .doesNotContainKey("openlineage");
  }

  // ---------- helpers ----------

  private static TableMetadata buildTableMetadata() {
    return TableMetadata.buildFromEmpty()
        .assignUUID("test-uuid")
        .setLocation("file:///tmp/warehouse/db/table")
        .addSchema(
            new Schema(
                List.of(
                    Types.NestedField.required(1, "id", Types.IntegerType.get(), "primary key"),
                    Types.NestedField.optional(2, "payload", Types.StringType.get()))))
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .build();
  }

  // ---------- test listeners ----------

  /** Captures the processed event; OpenLineage enabled (default null-config behaviour). */
  private static final class CaptureListener extends PolarisPersistenceEventListener {
    String realmId;
    org.apache.polaris.core.entity.PolarisEvent persistedEvent;

    @Override
    protected void processEvent(String realmId, org.apache.polaris.core.entity.PolarisEvent event) {
      this.realmId = realmId;
      this.persistedEvent = event;
    }
  }

  /** CaptureListener with OpenLineage explicitly disabled. */
  private static final class DisabledOpenLineageListener extends PolarisPersistenceEventListener {
    org.apache.polaris.core.entity.PolarisEvent persistedEvent;

    @Override
    protected boolean isOpenLineageEnabled() {
      return false;
    }

    @Override
    protected void processEvent(String realmId, org.apache.polaris.core.entity.PolarisEvent event) {
      this.persistedEvent = event;
    }
  }
}
