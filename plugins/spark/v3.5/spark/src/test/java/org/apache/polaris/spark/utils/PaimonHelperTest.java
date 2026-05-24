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
package org.apache.polaris.spark.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.apache.polaris.spark.NoopPaimonCatalog;
import org.apache.polaris.spark.PolarisSparkCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

public class PaimonHelperTest {
  private static final String TEST_WAREHOUSE = "file:///tmp/paimon/warehouse";

  @Test
  public void testLoadPaimonCatalogWithNoopPaimonCatalog() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);
    TableCatalog paimonCatalog = helper.loadPaimonCatalog();

    assertThat(paimonCatalog).isNotNull();
    assertThat(paimonCatalog).isInstanceOf(NoopPaimonCatalog.class);
    assertThat(paimonCatalog.name()).isEqualTo("paimon_paimon");
  }

  @Test
  public void testLoadPaimonCatalogCachesInstance() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);
    TableCatalog paimonCatalog1 = helper.loadPaimonCatalog();
    TableCatalog paimonCatalog2 = helper.loadPaimonCatalog();

    // Should return the same cached instance
    assertThat(paimonCatalog1).isSameAs(paimonCatalog2);
  }

  @Test
  public void testLoadPaimonCatalogWithNonExistentClass() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "com.example.NonExistentPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);
    assertThatThrownBy(() -> helper.loadPaimonCatalog())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot initialize Paimon Catalog")
        .hasMessageContaining("com.example.NonExistentPaimonCatalog");
  }

  @Test
  public void testLoadPaimonCatalogWithCaseInsensitiveOptions() {
    // Test that options are case-insensitive
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of("PAIMON-CATALOG-IMPL", "org.apache.polaris.spark.NoopPaimonCatalog"));
    PaimonHelper helper = new PaimonHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);

    TableCatalog paimonCatalog = helper.loadPaimonCatalog(polarisSparkCatalog);

    assertThat(paimonCatalog).isNotNull();
    assertThat(paimonCatalog).isInstanceOf(NoopPaimonCatalog.class);
  }

  @Test
  public void testLoadPaimonCatalogInitializesStandaloneCatalog() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            ImmutableMap.of(
                PaimonHelper.PAIMON_CATALOG_IMPL_KEY,
                "org.apache.polaris.spark.NoopPaimonCatalog",
                PaimonHelper.PAIMON_WAREHOUSE_KEY,
                TEST_WAREHOUSE));
    PaimonHelper helper = new PaimonHelper(options);
    PolarisSparkCatalog polarisSparkCatalog = mock(PolarisSparkCatalog.class);
    when(polarisSparkCatalog.name()).thenReturn("test");

    TableCatalog paimonCatalog = helper.loadPaimonCatalog(polarisSparkCatalog);

    assertThat(paimonCatalog).isInstanceOf(NoopPaimonCatalog.class);
    assertThat(paimonCatalog.name()).isEqualTo("paimon_paimon");
  }
}
