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

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

@ApplicationScoped
public class OpenLineageInputTracker {
  private static final Duration RETENTION = Duration.ofMinutes(10);

  private final Map<String, List<TrackedTableLoad>> trackedLoads = new ConcurrentHashMap<>();

  public void record(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    String sparkAppId = sparkAppId(tableMetadata);
    if (sparkAppId == null) {
      return;
    }

    Instant now = Instant.now();
    trackedLoads.compute(
        sparkAppId,
        (key, existing) -> {
          List<TrackedTableLoad> loads =
              existing == null ? new ArrayList<>() : new ArrayList<>(existing);
          loads.removeIf(load -> load.recordedAt().plus(RETENTION).isBefore(now));
          loads.add(new TrackedTableLoad(tableIdentifier, tableMetadata, now));
          return loads;
        });
  }

  public List<IcebergOpenLineageMapper.LineageDataset> inputsFor(
      TableIdentifier targetTableIdentifier, TableMetadata targetTableMetadata) {
    String sparkAppId = sparkAppId(targetTableMetadata);
    if (sparkAppId == null) {
      return List.of();
    }

    Instant now = Instant.now();
    Map<TableIdentifier, IcebergOpenLineageMapper.LineageDataset> deduped = new LinkedHashMap<>();
    for (TrackedTableLoad load : trackedLoads.getOrDefault(sparkAppId, List.of())) {
      if (load.recordedAt().plus(RETENTION).isBefore(now)) {
        continue;
      }
      if (load.tableIdentifier().equals(targetTableIdentifier)) {
        continue;
      }
      deduped.put(
          load.tableIdentifier(),
          new IcebergOpenLineageMapper.LineageDataset(
              load.tableIdentifier(), load.tableMetadata()));
    }
    return List.copyOf(deduped.values());
  }

  private static String sparkAppId(TableMetadata tableMetadata) {
    if (tableMetadata == null || tableMetadata.currentSnapshot() == null) {
      return null;
    }
    Map<String, String> summary = tableMetadata.currentSnapshot().summary();
    if (summary == null) {
      return null;
    }
    String sparkAppId = summary.get("spark.app.id");
    if (sparkAppId != null && !sparkAppId.isBlank()) {
      return sparkAppId;
    }
    String appId = summary.get("app-id");
    return appId == null || appId.isBlank() ? null : appId;
  }

  private record TrackedTableLoad(
      TableIdentifier tableIdentifier, TableMetadata tableMetadata, Instant recordedAt) {}
}
