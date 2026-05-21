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
package org.apache.polaris.spark.quarkus.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
public class SparkPaimonIT extends SparkIntegrationBase {

  private String defaultNs;
  private String tableRootDir;

  @Override
  protected SparkSession buildSparkSession() {
    return SparkSession.builder()
        .master("local[1]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.polaris.spark.SparkCatalog")
        .config("spark.sql.warehouse.dir", warehouseDir.toString())
        .config(String.format("spark.sql.catalog.%s.type", catalogName), "rest")
        .config(
            String.format("spark.sql.catalog.%s.uri", catalogName),
            endpoints.catalogApiEndpoint().toString())
        .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), catalogName)
        .config(String.format("spark.sql.catalog.%s.scope", catalogName), "PRINCIPAL_ROLE:ALL")
        .config(
            String.format("spark.sql.catalog.%s.header.realm", catalogName), endpoints.realmId())
        .config(String.format("spark.sql.catalog.%s.token", catalogName), sparkToken)
        .config(String.format("spark.sql.catalog.%s.s3.access-key-id", catalogName), "fakekey")
        .config(
            String.format("spark.sql.catalog.%s.s3.secret-access-key", catalogName), "fakesecret")
        .config(String.format("spark.sql.catalog.%s.s3.region", catalogName), "us-west-2")
        .config(
            String.format("spark.sql.catalog.%s.paimon-warehouse", catalogName),
            warehouseDir.resolve("paimon/").toString())
        .getOrCreate();
  }

  private String getTableLocation(String tableName) {
    return warehouseDir.resolve(String.format("paimon/%s.db/%s", defaultNs, tableName)).toString();
  }

  private String getLocalTableLocation(String tableName) {
    return Path.of(URI.create(getTableLocation(tableName))).toString();
  }

  private String getTableNameWithRandomSuffix() {
    return generateName("paimontb");
  }

  @BeforeEach
  public void createDefaultResources() {
    spark.sparkContext().setLogLevel("WARN");
    defaultNs = generateName("paimon");
    sql("CREATE NAMESPACE %s", defaultNs);
    sql("USE NAMESPACE %s", defaultNs);
    tableRootDir = Path.of(URI.create(warehouseDir.resolve("paimon/").toString())).toString();
  }

  @AfterEach
  public void cleanupPaimonData() {
    if (tableRootDir != null) {
      FileUtils.deleteQuietly(new File(tableRootDir));
    }
    if (defaultNs != null) {
      sql("DROP NAMESPACE %s", defaultNs);
    }
  }

  @Test
  public void testBasicTableOperations() {
    String paimontb1 = "paimontb1";
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING PAIMON LOCATION '%s'",
        paimontb1, getTableLocation(paimontb1));
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", paimontb1);

    List<Object[]> results = sql("SELECT id, name FROM %s WHERE id > 1", paimontb1);
    assertThat(results).containsExactly(new Object[] {2, "bob"});

    String paimontb2 = "paimontb2";
    sql(
        "CREATE TABLE %s (name STRING, age INT, country STRING) USING PAIMON PARTITIONED BY (country) LOCATION '%s'",
        paimontb2, getTableLocation(paimontb2));
    sql(
        "INSERT INTO %s VALUES ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA')",
        paimontb2);

    results = sql("SELECT name, country FROM %s ORDER BY age", paimontb2);
    assertThat(results)
        .containsExactly(
            new Object[] {"anna", "US"},
            new Object[] {"yan", "CHINA"},
            new Object[] {"james", "US"});

    List<String> subDirs = listDirs(getLocalTableLocation(paimontb2));
    assertThat(subDirs).contains("manifest", "schema", "snapshot");

    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables)
        .contains(
            new Object[] {defaultNs, paimontb1, false}, new Object[] {defaultNs, paimontb2, false});

    sql("DROP TABLE %s", paimontb1);
    sql("DROP TABLE %s", paimontb2);
    assertThat(sql("SHOW TABLES")).isEmpty();
  }

  @Test
  public void testUnsupportedTableCreateOperations() {
    String paimontb = getTableNameWithRandomSuffix();

    assertThatThrownBy(() -> sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", paimontb))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testAlterOperations() {
    String paimontb = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (id INT, name STRING) USING PAIMON LOCATION '%s'",
        paimontb, getTableLocation(paimontb));
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", paimontb);

    sql("ALTER TABLE %s ADD COLUMNS (age INT)", paimontb);
    sql("INSERT INTO %s VALUES (3, 'john', 20)", paimontb);

    List<Object[]> results = sql("SELECT id, name, age FROM %s ORDER BY id", paimontb);
    assertThat(results)
        .containsExactly(
            new Object[] {1, "anna", null},
            new Object[] {2, "bob", null},
            new Object[] {3, "john", 20});

    sql("DROP TABLE %s", paimontb);
  }
}
