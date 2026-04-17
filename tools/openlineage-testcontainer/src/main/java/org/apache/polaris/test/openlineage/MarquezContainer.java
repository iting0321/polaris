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

package org.apache.polaris.test.openlineage;

import java.time.Duration;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public final class MarquezContainer extends GenericContainer<MarquezContainer> {
  static final int HOST_API_PORT = 5000;
  static final int HOST_ADMIN_PORT = 5001;
  static final int API_PORT = 5000;
  static final int ADMIN_PORT = 5001;

  public MarquezContainer(Network network) {
    super(resolveImage());
    withNetwork(network);
    withNetworkAliases("marquez");
    addFixedExposedPort(HOST_API_PORT, API_PORT);
    addFixedExposedPort(HOST_ADMIN_PORT, ADMIN_PORT);
    withEnv("POSTGRES_HOST", "postgres");
    withEnv("POSTGRES_PORT", "5432");
    withEnv("POSTGRES_DB", "marquez");
    withEnv("POSTGRES_USER", "marquez");
    withEnv("POSTGRES_PASSWORD", "marquez");
    withEnv("SEARCH_ENABLED", "false");
    withEnv("MARQUEZ_PORT", Integer.toString(API_PORT));
    withEnv("MARQUEZ_ADMIN_PORT", Integer.toString(ADMIN_PORT));
    waitingFor(Wait.forHttp("/ping").forPort(ADMIN_PORT).withStartupTimeout(Duration.ofMinutes(2)));
  }

  public String apiUrl() {
    return "http://" + getHost() + ':' + HOST_API_PORT;
  }

  public String lineageUrl() {
    return apiUrl() + "/api/v1/lineage";
  }

  public String adminUrl() {
    return "http://" + getHost() + ':' + HOST_ADMIN_PORT;
  }

  private static DockerImageName resolveImage() {
    return ContainerSpecHelper.containerSpecHelper("marquez", MarquezContainer.class)
        .dockerImageName(null);
  }
}
