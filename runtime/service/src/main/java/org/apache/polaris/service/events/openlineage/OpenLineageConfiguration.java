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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Configuration for OpenLineage dataset event emission.
 *
 * <p>OpenLineage facets are embedded as a JSON string in the {@code openlineage} key of each
 * persisted {@code PolarisEvent}'s additional properties, for table create, update, and drop
 * operations.
 */
@StaticInitSafe
@ConfigMapping(prefix = "polaris.openlineage")
public interface OpenLineageConfiguration {

  /**
   * Whether OpenLineage dataset facet emission is enabled. When disabled, no {@code openlineage}
   * key is written to event additional properties.
   *
   * <p>Default: {@code false}
   */
  @WithDefault("false")
  boolean enabled();

  /**
   * The producer URI embedded in every OpenLineage facet, identifying the system that generated the
   * event.
   *
   * <p>Default: {@code https://github.com/apache/polaris}
   */
  @WithDefault("https://github.com/apache/polaris")
  String producer();
}
