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
package org.apache.polaris.spark;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;

/**
 * Test-only Paimon catalog that behaves like a standalone Spark catalog instead of a delegating
 * catalog.
 */
public class NoopPaimonCatalog implements TableCatalog, SupportsNamespaces {

  private String catalogName;
  private final Map<String, Map<String, String>> namespaces = new HashMap<>();
  private final Map<String, NoopPaimonTable> tables = new HashMap<>();

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    String namespaceKey = namespaceKey(namespace);
    return tables.keySet().stream()
        .filter(key -> key.startsWith(namespaceKey + "."))
        .map(key -> Identifier.of(namespace, key.substring(namespaceKey.length() + 1)))
        .toArray(Identifier[]::new);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    NoopPaimonTable table = tables.get(tableKey(ident));
    if (table == null) {
      throw new NoSuchTableException(ident);
    }
    return table;
  }

  @Override
  @SuppressWarnings("deprecation")
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    if (!namespaceExists(ident.namespace())) {
      throw new NoSuchNamespaceException(ident.namespace());
    }
    String tableKey = tableKey(ident);
    if (tables.containsKey(tableKey)) {
      throw new TableAlreadyExistsException(ident);
    }
    NoopPaimonTable table = new NoopPaimonTable(ident.name(), schema, properties);
    tables.put(tableKey, table);
    return table;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return loadTable(ident);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return tables.remove(tableKey(ident)) != null;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException {
    NoopPaimonTable table = tables.remove(tableKey(oldIdent));
    if (table == null) {
      throw new NoSuchTableException(oldIdent);
    }
    tables.put(tableKey(newIdent), table);
  }

  @Override
  public String[][] listNamespaces() {
    return namespaces.keySet().stream()
        .map(namespace -> namespace.split("\\."))
        .toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    String namespaceKey = namespaceKey(namespace);
    return namespaces.keySet().stream()
        .filter(key -> key.startsWith(namespaceKey + "."))
        .map(key -> key.substring(namespaceKey.length() + 1))
        .filter(key -> !key.contains("."))
        .map(
            child -> {
              String[] childNamespace = new String[namespace.length + 1];
              System.arraycopy(namespace, 0, childNamespace, 0, namespace.length);
              childNamespace[namespace.length] = child;
              return childNamespace;
            })
        .toArray(String[][]::new);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    Map<String, String> metadata = namespaces.get(namespaceKey(namespace));
    if (metadata == null) {
      throw new NoSuchNamespaceException(namespace);
    }
    return metadata;
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    String namespaceKey = namespaceKey(namespace);
    if (namespaces.containsKey(namespaceKey)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
    namespaces.put(namespaceKey, new HashMap<>(metadata));
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    return namespaces.remove(namespaceKey(namespace)) != null;
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return namespaces.containsKey(namespaceKey(namespace));
  }

  private String tableKey(Identifier ident) {
    return namespaceKey(ident.namespace()) + "." + ident.name();
  }

  private String namespaceKey(String[] namespace) {
    return String.join(".", namespace);
  }

  private static class NoopPaimonTable implements Table {
    private final String name;
    private final StructType schema;
    private final Map<String, String> properties;

    NoopPaimonTable(String name, StructType schema, Map<String, String> properties) {
      this.name = name;
      this.schema = schema;
      this.properties = new HashMap<>(properties);
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    @SuppressWarnings("deprecation")
    public StructType schema() {
      return schema;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public Set<TableCapability> capabilities() {
      return new HashSet<>();
    }
  }
}
