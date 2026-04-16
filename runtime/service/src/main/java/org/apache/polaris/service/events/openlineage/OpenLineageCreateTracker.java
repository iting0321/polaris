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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.catalog.TableIdentifier;

@ApplicationScoped
public class OpenLineageCreateTracker {
  private static final Duration RETENTION = Duration.ofMinutes(5);

  private final Map<TableIdentifier, TrackedCreate> trackedCreates = new ConcurrentHashMap<>();

  public void record(TableIdentifier tableIdentifier, String requestId) {
    trackedCreates.put(tableIdentifier, new TrackedCreate(requestId, Instant.now()));
  }

  public boolean consumeIfMatches(TableIdentifier tableIdentifier, String requestId) {
    Instant now = Instant.now();
    TrackedCreate trackedCreate = trackedCreates.get(tableIdentifier);
    if (trackedCreate == null) {
      return false;
    }
    if (trackedCreate.recordedAt().plus(RETENTION).isBefore(now)) {
      trackedCreates.remove(tableIdentifier, trackedCreate);
      return false;
    }
    if (trackedCreate.requestId() == null || requestId == null) {
      return false;
    }
    if (!trackedCreate.requestId().equals(requestId)) {
      return false;
    }
    return trackedCreates.remove(tableIdentifier, trackedCreate);
  }

  private record TrackedCreate(String requestId, Instant recordedAt) {}
}
