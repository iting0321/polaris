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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.lineage.LineageDataset;
import org.apache.polaris.core.lineage.LineageEdge;
import org.apache.polaris.core.lineage.LineageIngestRequest;
import org.apache.polaris.core.lineage.LineageService;
import org.apache.polaris.service.lineage.api.PolarisLineageEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OpenLineageAdapterTest {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";

  @Test
  void sendLineageEventPersistsRunEventDatasetGraph() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    LineageService lineageService = mock(LineageService.class);
    OpenLineageAdapter adapter = new OpenLineageAdapter(lineageService);

    PolarisLineageEvent event =
        objectMapper.readValue(
            """
            {
              "eventType": "COMPLETE",
              "eventTime": "2026-06-10T01:02:03Z",
              "run": {"runId": "11111111-1111-1111-1111-111111111111"},
              "job": {"namespace": "spark", "name": "daily_orders"},
              "producer": "https://polaris.apache.org",
              "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
              "inputs": [
                {"namespace": "file://warehouse", "name": "raw/orders"},
                {"namespace": "postgres://db", "name": "public/customers"}
              ],
              "outputs": [
                {"namespace": "polaris://catalog", "name": "analytics/orders_daily"}
              ]
            }
            """,
            PolarisLineageEvent.class);

    adapter.sendLineageEvent(event, REALM_CONTEXT, null);

    ArgumentCaptor<LineageIngestRequest> requestCaptor =
        ArgumentCaptor.forClass(LineageIngestRequest.class);
    verify(lineageService).ingest(requestCaptor.capture());
    LineageIngestRequest request = requestCaptor.getValue();

    LineageDataset rawOrders = dataset("file://warehouse", "raw/orders");
    LineageDataset customers = dataset("postgres://db", "public/customers");
    LineageDataset ordersDaily = dataset("polaris://catalog", "analytics/orders_daily");
    assertThat(request.datasets()).containsExactly(rawOrders, customers, ordersDaily);
    assertThat(request.edges())
        .containsExactly(
            new LineageEdge(rawOrders, ordersDaily), new LineageEdge(customers, ordersDaily));
    assertThat(request.columnEdges()).isEmpty();
    assertThat(request.eventTime()).contains(Instant.parse("2026-06-10T01:02:03Z"));
  }

  private static LineageDataset dataset(String namespace, String name) {
    return new LineageDataset("", namespace, name);
  }
}
