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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.polaris.test.openlineage;

import java.time.Duration;
import org.apache.polaris.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public final class MarquezWebContainer extends GenericContainer<MarquezWebContainer> {
  private static final int HOST_WEB_PORT = 3000;
  private static final int WEB_PORT = 3000;

  public MarquezWebContainer(Network network) {
    super(resolveImage());
    withNetwork(network);
    withNetworkAliases("marquez-web");
    addFixedExposedPort(HOST_WEB_PORT, WEB_PORT);
    withEnv("MARQUEZ_HOST", "marquez");
    withEnv("MARQUEZ_PORT", Integer.toString(MarquezContainer.API_PORT));
    withEnv("WEB_PORT", Integer.toString(WEB_PORT));
    waitingFor(Wait.forHttp("/").forPort(WEB_PORT).withStartupTimeout(Duration.ofMinutes(2)));
  }

  public String webUrl() {
    return "http://" + getHost() + ':' + HOST_WEB_PORT;
  }

  private static DockerImageName resolveImage() {
    return ContainerSpecHelper.containerSpecHelper("marquez-web", MarquezWebContainer.class)
        .dockerImageName(null);
  }
}
