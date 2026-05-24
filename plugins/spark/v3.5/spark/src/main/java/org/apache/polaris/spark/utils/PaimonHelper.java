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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.polaris.spark.PolarisSparkCatalog;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Helper class for integrating Apache Paimon table functionality with Polaris Spark Catalog.
 *
 * <p>This class is responsible for dynamically loading and configuring a Paimon Catalog
 * implementation to work with Polaris. The default Paimon Spark catalog is a standalone catalog
 * that must be initialized with its own catalog options before it can create Paimon metadata. When
 * the loaded catalog also supports Spark's catalog extension APIs, this helper wires Polaris as the
 * delegate catalog.
 *
 * <p>Apache Paimon is a streaming data lake platform with high-speed data ingestion, changelog
 * tracking and efficient real-time analytics. This helper enables Polaris to manage Paimon tables
 * alongside Iceberg, Delta, and Hudi tables in a unified catalog.
 */
public class PaimonHelper {
  public static final String PAIMON_CATALOG_IMPL_KEY = "paimon-catalog-impl";
  public static final String PAIMON_WAREHOUSE_KEY = "paimon-warehouse";
  private static final String DEFAULT_PAIMON_CATALOG_CLASS = "org.apache.paimon.spark.SparkCatalog";

  private TableCatalog paimonCatalog = null;
  private final String paimonCatalogImpl;
  private final String catalogName;
  private final CaseInsensitiveStringMap options;

  public PaimonHelper(String catalogName, CaseInsensitiveStringMap options) {
    this.catalogName = catalogName;
    this.options = options;
    this.paimonCatalogImpl =
        options.get(PAIMON_CATALOG_IMPL_KEY) != null
            ? options.get(PAIMON_CATALOG_IMPL_KEY)
            : DEFAULT_PAIMON_CATALOG_CLASS;
  }

  public PaimonHelper(CaseInsensitiveStringMap options) {
    this("paimon", options);
  }

  /**
   * Load and configure the Paimon catalog.
   *
   * @return the configured Paimon TableCatalog
   */
  public synchronized TableCatalog loadPaimonCatalog() {
    if (this.paimonCatalog != null) {
      return this.paimonCatalog;
    }

    TableCatalog catalog;
    DynConstructors.Ctor<TableCatalog> ctor;
    try {
      ctor = DynConstructors.builder(TableCatalog.class).impl(paimonCatalogImpl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog %s: %s", paimonCatalogImpl, e.getMessage()),
          e);
    }

    try {
      catalog = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize Paimon Catalog, %s does not implement TableCatalog.",
              paimonCatalogImpl),
          e);
    }

    catalog.initialize(this.catalogName + "_paimon", paimonCatalogOptions());
    this.paimonCatalog = catalog;

    return this.paimonCatalog;
  }

  public synchronized TableCatalog loadPaimonCatalog(PolarisSparkCatalog polarisSparkCatalog) {
    TableCatalog catalog = loadPaimonCatalog();
    if (catalog instanceof DelegatingCatalogExtension delegatingCatalogExtension) {
      delegatingCatalogExtension.setDelegateCatalog(polarisSparkCatalog);
    } else if (catalog instanceof CatalogExtension catalogExtension) {
      catalogExtension.setDelegateCatalog(polarisSparkCatalog);
    }
    return catalog;
  }

  public void ensureNamespaceExists(String[] namespace) {
    if (namespace.length == 0) {
      return;
    }
    if (this.paimonCatalog instanceof SupportsNamespaces supportsNamespaces
        && !supportsNamespaces.namespaceExists(namespace)) {
      try {
        supportsNamespaces.createNamespace(namespace, new HashMap<>());
      } catch (Exception e) {
        // Namespace might already exist due to a concurrent create.
      }
    }
  }

  private CaseInsensitiveStringMap paimonCatalogOptions() {
    Map<String, String> paimonOptions = new HashMap<>(options.asCaseSensitiveMap());
    paimonOptions.remove(PAIMON_CATALOG_IMPL_KEY);
    paimonOptions.remove(PAIMON_WAREHOUSE_KEY);
    String paimonWarehouse = options.get(PAIMON_WAREHOUSE_KEY);
    if (paimonWarehouse != null) {
      paimonOptions.put("warehouse", paimonWarehouse);
    }
    return new CaseInsensitiveStringMap(paimonOptions);
  }
}
