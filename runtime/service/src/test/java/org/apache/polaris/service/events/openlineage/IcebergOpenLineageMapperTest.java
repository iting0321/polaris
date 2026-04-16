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

package org.apache.polaris.service.events.openlineage;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.service.events.PolarisEventType;
import org.junit.jupiter.api.Test;

class IcebergOpenLineageMapperTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final URI TEST_PRODUCER = URI.create("https://github.com/apache/polaris");

  @Test
  void createRunEventContainsSyntheticJobAndDatasetFacets() throws Exception {
    Snapshot snapshot =
        new StubSnapshot(42L, "replace", Map.of("total-records", "10", "total-file-size", "2048"));
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("test-uuid")
            .setLocation("file:///tmp/warehouse/db/table")
            .addSchema(
                new Schema(
                    List.of(
                        Types.NestedField.required(1, "id", Types.IntegerType.get(), "primary key"),
                        Types.NestedField.optional(2, "payload", Types.StringType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .setBranchSnapshot(snapshot, "main")
            .build();

    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toTableRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                PolarisEventType.AFTER_CREATE_TABLE,
                "req-42",
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(),
                TableIdentifier.of("db_sales", "daily_orders"),
                tableMetadata));

    assertThat(event.path("eventType").asText()).isEqualTo("COMPLETE");
    assertThat(event.at("/job/namespace").asText()).isEqualTo("polaris.POLARIS.catalog1");
    assertThat(event.at("/job/name").asText()).isEqualTo("after_create_table:db_sales.daily_orders");
    assertThat(event.at("/run/facets/processing_engine/name").asText()).isEqualTo("polaris");
    assertThat(event.at("/outputs/0/namespace").asText()).isEqualTo("db_sales");
    assertThat(event.at("/outputs/0/name").asText()).isEqualTo("daily_orders");
    assertThat(event.at("/outputs/0/facets/schema/fields")).hasSize(2);
    assertThat(event.at("/outputs/0/facets/schema/fields/0/name").asText()).isEqualTo("id");
    assertThat(event.at("/outputs/0/facets/schema/fields/0/type").asText()).isEqualTo("int");
    assertThat(event.at("/outputs/0/facets/schema/fields/0/description").asText())
        .isEqualTo("primary key");
    assertThat(event.at("/outputs/0/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("CREATE");
    assertThat(event.at("/outputs/0/facets/version/datasetVersion").asText()).isEqualTo("42");
    assertThat(event.at("/outputs/0/facets/schema/_producer").asText())
        .isEqualTo(TEST_PRODUCER.toString());
  }

  @Test
  void updateRunEventIncludesInputAndOutputDatasets() throws Exception {
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("test-uuid")
            .setLocation("file:///tmp/warehouse/db/table")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();

    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toTableRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                PolarisEventType.AFTER_UPDATE_TABLE,
                "req-43",
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(),
                TableIdentifier.of("db_sales", "daily_orders"),
                tableMetadata));

    assertThat(event.path("eventType").asText()).isEqualTo("COMPLETE");
    assertThat(event.at("/inputs/0/namespace").asText()).isEqualTo("db_sales");
    assertThat(event.at("/inputs/0/name").asText()).isEqualTo("daily_orders");
    assertThat(event.at("/outputs/0/namespace").asText()).isEqualTo("db_sales");
    assertThat(event.at("/outputs/0/name").asText()).isEqualTo("daily_orders");
    assertThat(event.at("/outputs/0/facets/lifecycleStateChange").isMissingNode()).isTrue();
  }

  @Test
  void updateRunEventUsesAlterLifecycleEvenWhenOnlyFirstSnapshotExists() throws Exception {
    Snapshot snapshot =
        new StubSnapshot(42L, "append", Map.of("total-records", "10", "total-file-size", "2048"));
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("test-uuid")
            .setLocation("file:///tmp/warehouse/db/table")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .setBranchSnapshot(snapshot, "main")
            .build();

    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toTableRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                PolarisEventType.AFTER_UPDATE_TABLE,
                "req-43b",
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(),
                TableIdentifier.of("db_sales", "daily_orders"),
                tableMetadata));

    assertThat(event.at("/outputs/0/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("ALTER");
  }

  @Test
  void updateRunEventUsesOverwriteLifecycleForOverwriteSnapshots() throws Exception {
    Snapshot snapshot =
        new StubSnapshot(
            43L, "overwrite", Map.of("total-records", "10", "total-file-size", "2048"));
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("test-uuid")
            .setLocation("file:///tmp/warehouse/db/table")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .setBranchSnapshot(snapshot, "main")
            .build();

    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toTableRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                PolarisEventType.AFTER_UPDATE_TABLE,
                "req-43c",
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(),
                TableIdentifier.of("db_sales", "daily_orders"),
                tableMetadata));

    assertThat(event.at("/outputs/0/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("OVERWRITE");
  }

  @Test
  void createRunEventInputDatasetsDoNotExposeLifecycleForLoadEvents() throws Exception {
    Snapshot sourceSnapshot =
        new StubSnapshot(55L, "append", Map.of("total-records", "2", "total-file-size", "1024"));
    TableMetadata sourceTableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("source-uuid")
            .setLocation("file:///tmp/warehouse/db/source")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .setBranchSnapshot(sourceSnapshot, "main")
            .build();
    Snapshot targetSnapshot =
        new StubSnapshot(56L, "append", Map.of("total-records", "2", "total-file-size", "1024"));
    TableMetadata targetTableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("target-uuid")
            .setLocation("file:///tmp/warehouse/db/target")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .setBranchSnapshot(targetSnapshot, "main")
            .build();

    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toTableRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                PolarisEventType.AFTER_CREATE_TABLE,
                "req-43d",
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(
                    new IcebergOpenLineageMapper.LineageDataset(
                        TableIdentifier.of("db_src", "orders_raw"), sourceTableMetadata)),
                TableIdentifier.of("db_out", "sales_report"),
                targetTableMetadata));

    assertThat(event.at("/inputs/0/facets/version/datasetVersion").asText()).isEqualTo("55");
    assertThat(event.at("/inputs/0/facets/lifecycleStateChange").isMissingNode()).isTrue();
    assertThat(event.at("/outputs/0/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("CREATE");
  }

  @Test
  void omitsSnapshotFacetsWhenCurrentSnapshotIsMissing() throws Exception {
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID("test-uuid")
            .setLocation("file:///tmp/warehouse/db/table")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();

    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toTableRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                PolarisEventType.AFTER_CREATE_TABLE,
                "req-44",
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(),
                TableIdentifier.of("db_sales", "daily_orders"),
                tableMetadata));

    assertThat(event.at("/outputs/0/facets/schema/fields")).hasSize(1);
    assertThat(event.at("/outputs/0/facets/lifecycleStateChange").isMissingNode()).isTrue();
    assertThat(event.at("/outputs/0/facets/version").isMissingNode()).isTrue();
  }

  @Test
  void dropRunJsonContainsDropLifecycleState() throws Exception {
    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toDropRunEventJson(
                TEST_PRODUCER,
                "catalog1",
                "POLARIS",
                "req-45",
                Instant.parse("2026-01-01T00:00:00Z"),
                TableIdentifier.of("db_sales", "daily_orders")));

    assertThat(event.path("eventType").asText()).isEqualTo("COMPLETE");
    assertThat(event.at("/job/namespace").asText()).isEqualTo("polaris.POLARIS.catalog1");
    assertThat(event.at("/job/name").asText()).isEqualTo("after_drop_table:db_sales.daily_orders");
    assertThat(event.at("/inputs/0/namespace").asText()).isEqualTo("db_sales");
    assertThat(event.at("/inputs/0/name").asText()).isEqualTo("daily_orders");
    assertThat(event.at("/inputs/0/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("DROP");
    assertThat(event.at("/outputs")).hasSize(0);
  }

  private record StubSnapshot(long snapshotId, String operation, Map<String, String> summary)
      implements Snapshot {
    @Override
    public long sequenceNumber() {
      return 1L;
    }

    @Override
    public Long parentId() {
      return null;
    }

    @Override
    public long timestampMillis() {
      return 1L;
    }

    @Override
    public List<org.apache.iceberg.ManifestFile> allManifests(FileIO io) {
      return List.of();
    }

    @Override
    public List<org.apache.iceberg.ManifestFile> dataManifests(FileIO io) {
      return List.of();
    }

    @Override
    public List<org.apache.iceberg.ManifestFile> deleteManifests(FileIO io) {
      return List.of();
    }

    @Override
    public Iterable<DataFile> addedDataFiles(FileIO io) {
      return List.of();
    }

    @Override
    public Iterable<DataFile> removedDataFiles(FileIO io) {
      return List.of();
    }

    @Override
    public String manifestListLocation() {
      return "file:///tmp/manifest-list.avro";
    }
  }
}
