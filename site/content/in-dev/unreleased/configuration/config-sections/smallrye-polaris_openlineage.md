---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_openlineage
build:
  list: never
  render: never
---

Configuration for OpenLineage dataset event emission. 

OpenLineage facets are embedded as a JSON string in the `openlineage` key of each  persisted `PolarisEvent`'s additional properties, for table create, update, and drop  operations.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.openlineage.enabled` | `true` | `boolean` | Whether OpenLineage dataset facet emission is enabled. When disabled, no `openlineage` key is written to event additional properties.   <br><br>Default: `true` |
| `polaris.openlineage.producer` | `https://github.com/apache/polaris` | `string` | The producer URI embedded in every OpenLineage facet, identifying the system that generated the  event.  <br><br>Default: `https://github.com/apache/polaris` |
