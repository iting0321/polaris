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
import com.fasterxml.jackson.databind.SerializationFeature;
import java.net.URI;
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
import org.junit.jupiter.api.Test;

class IcebergOpenLineageMapperTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final URI TEST_PRODUCER = URI.create("https://github.com/apache/polaris");

  @Test
  void demo() throws Exception {
    var prettyMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // --- Case 1: table with snapshot ---
    Snapshot snapshot =
        new StubSnapshot(
            99L, "replace", Map.of("total-records", "500", "total-file-size", "10240"));
    TableMetadata withSnapshot =
        TableMetadata.buildFromEmpty()
            .assignUUID("demo-uuid")
            .setLocation("s3://warehouse/sales/orders")
            .addSchema(
                new Schema(
                    List.of(
                        Types.NestedField.required(1, "order_id", Types.LongType.get(), "PK"),
                        Types.NestedField.optional(2, "customer", Types.StringType.get()),
                        Types.NestedField.optional(
                            3, "amount", Types.DecimalType.of(10, 2), "USD"))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .setBranchSnapshot(snapshot, "main")
            .build();

    String json1 =
        IcebergOpenLineageMapper.toDatasetEventJson(
            TEST_PRODUCER, TableIdentifier.of("sales", "orders"), withSnapshot);
    System.out.println(
        "=== [Case 1] Table WITH snapshot (lifecycleStateChange=CREATE, first snapshot) ===");
    System.out.println(prettyMapper.readTree(json1).toPrettyString());

    // --- Case 2: freshly created table, no snapshot yet ---
    TableMetadata noSnapshot =
        TableMetadata.buildFromEmpty()
            .assignUUID("new-uuid")
            .setLocation("s3://warehouse/sales/events")
            .addSchema(
                new Schema(List.of(Types.NestedField.required(1, "id", Types.IntegerType.get()))))
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();

    String json2 =
        IcebergOpenLineageMapper.toDatasetEventJson(
            TEST_PRODUCER, TableIdentifier.of("sales", "events"), noSnapshot);
    System.out.println("\n=== [Case 2] Table WITHOUT snapshot ===");
    System.out.println(prettyMapper.readTree(json2).toPrettyString());

    // --- Case 3: drop table ---
    String json3 =
        IcebergOpenLineageMapper.toDropDatasetEventJson(
            TEST_PRODUCER, TableIdentifier.of("sales", "orders"));
    System.out.println("\n=== [Case 3] DROP table ===");
    System.out.println(prettyMapper.readTree(json3).toPrettyString());
  }

  @Test
  void mapsSchemaAndSnapshotFacets() throws Exception {
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
            IcebergOpenLineageMapper.toDatasetEventJson(
                TEST_PRODUCER, TableIdentifier.of("db_sales", "daily_orders"), tableMetadata));

    assertThat(event.at("/dataset/namespace").asText()).isEqualTo("db_sales");
    assertThat(event.at("/dataset/name").asText()).isEqualTo("daily_orders");
    assertThat(event.at("/dataset/facets/schema/fields")).hasSize(2);
    assertThat(event.at("/dataset/facets/schema/fields/0/name").asText()).isEqualTo("id");
    assertThat(event.at("/dataset/facets/schema/fields/0/type").asText()).isEqualTo("int");
    assertThat(event.at("/dataset/facets/schema/fields/0/description").asText())
        .isEqualTo("primary key");
    // First snapshot → lifecycleStateChange=CREATE
    assertThat(event.at("/dataset/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("CREATE");
    assertThat(event.at("/dataset/facets/version/datasetVersion").asText()).isEqualTo("42");
    // SDK embeds _producer in every facet
    assertThat(event.at("/dataset/facets/schema/_producer").asText())
        .isEqualTo(TEST_PRODUCER.toString());
    // eventTime and eventType must be present at the event envelope level
    assertThat(event.path("eventTime").isMissingNode()).isFalse();
    assertThat(event.path("eventType").asText()).isEqualTo("DATASET");
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
            IcebergOpenLineageMapper.toDatasetEventJson(
                TEST_PRODUCER, TableIdentifier.of("db_sales", "daily_orders"), tableMetadata));

    assertThat(event.at("/dataset/facets/schema/fields")).hasSize(1);
    assertThat(event.at("/dataset/facets/lifecycleStateChange").isMissingNode()).isTrue();
    assertThat(event.at("/dataset/facets/version").isMissingNode()).isTrue();
  }

  @Test
  void dropDatasetJsonContainsDropLifecycleState() throws Exception {
    var event =
        OBJECT_MAPPER.readTree(
            IcebergOpenLineageMapper.toDropDatasetEventJson(
                TEST_PRODUCER, TableIdentifier.of("db_sales", "daily_orders")));

    assertThat(event.at("/dataset/namespace").asText()).isEqualTo("db_sales");
    assertThat(event.at("/dataset/name").asText()).isEqualTo("daily_orders");
    assertThat(event.path("eventType").asText()).isEqualTo("DATASET");
    assertThat(event.at("/dataset/facets/lifecycleStateChange/lifecycleStateChange").asText())
        .isEqualTo("DROP");
    assertThat(event.at("/dataset/facets/schema").isMissingNode()).isTrue();
    assertThat(event.at("/dataset/facets/version").isMissingNode()).isTrue();
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
