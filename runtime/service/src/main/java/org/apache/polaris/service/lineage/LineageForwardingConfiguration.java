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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithDefaults;
import io.smallrye.config.WithName;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@StaticInitSafe
@ConfigMapping(prefix = "polaris.lineage.forwarding")
public interface LineageForwardingConfiguration {

  @WithDefault("false")
  boolean enabled();

  /** Names of target entries under {@code polaris.lineage.forwarding.target.<name>}. */
  Optional<Set<String>> targets();

  @WithName("target")
  Map<String, TargetConfiguration> targetConfigurations();

  interface TargetConfiguration {
    URI url();

    @WithDefault("api/v1/lineage")
    String endpoint();

    @WithDefault("PT5S")
    Duration timeout();

    @WithName("failure-mode")
    @WithDefault("fail-closed")
    FailureMode failureMode();

    @WithDefaults
    AuthConfiguration auth();
  }

  interface AuthConfiguration {
    @WithDefault("none")
    AuthType type();

    @WithName("api-key")
    Optional<String> apiKey();
  }

  enum FailureMode {
    FAIL_OPEN,
    FAIL_CLOSED
  }

  enum AuthType {
    NONE,
    API_KEY
  }
}
