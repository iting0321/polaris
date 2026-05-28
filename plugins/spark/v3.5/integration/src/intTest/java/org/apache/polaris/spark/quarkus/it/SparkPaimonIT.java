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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.util.List;
import org.apache.polaris.service.it.ext.SparkSessionBuilder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
public class SparkPaimonIT extends SparkIntegrationBase {
  private static final String PAIMON_DELEGATING_CATALOG =
      "org.apache.paimon.spark.SparkDelegatingGenericCatalog";
  private static final String PAIMON_EXTENSIONS =
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions";

  @BeforeAll
  public static void requirePaimonDelegatingCatalog() {
    assumeTrue(classAvailable(PAIMON_DELEGATING_CATALOG), "Paimon delegating catalog not found");
    assumeTrue(classAvailable(PAIMON_EXTENSIONS), "Paimon Spark extensions not found");
  }

  @Override
  protected SparkSession buildSparkSession() {
    return SparkSessionBuilder.buildWithTestDefaults()
        .withExtensions(
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                + PAIMON_EXTENSIONS)
        .withWarehouse(warehouseDir)
        .addCatalog(catalogName, "org.apache.polaris.spark.SparkCatalog", endpoints, sparkToken)
        .withConfig(
            String.format("spark.sql.catalog.%s.paimon-catalog-impl", catalogName),
            PAIMON_DELEGATING_CATALOG)
        .getOrCreate();
  }

  @BeforeEach
  public void createDefaultResources() {
    spark.sparkContext().setLogLevel("WARN");
    sql("CREATE NAMESPACE IF NOT EXISTS default");
    sql("USE NAMESPACE default");
  }

  private String getTableNameWithRandomSuffix() {
    return generateName("paimontb");
  }

  @Test
  public void testBasicTableOperations() {
    String paimontb1 = getTableNameWithRandomSuffix();
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", paimontb1);
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", paimontb1);

    List<Object[]> results =
        sql("SELECT id, name FROM %s WHERE id > 1 ORDER BY id DESC", paimontb1);
    assertThat(results.size()).isEqualTo(1);
    assertThat(results.get(0)).isEqualTo(new Object[] {2, "bob"});

    String paimontb2 = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (name STRING, age INT, country STRING) USING PAIMON PARTITIONED BY (country)",
        paimontb2);
    sql(
        "INSERT INTO %s VALUES ('anna', 10, 'US'), ('james', 32, 'US'), ('yan', 16, 'CHINA')",
        paimontb2);

    results = sql("SELECT name, country FROM %s ORDER BY age", paimontb2);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results.get(0)).isEqualTo(new Object[] {"anna", "US"});
    assertThat(results.get(1)).isEqualTo(new Object[] {"yan", "CHINA"});
    assertThat(results.get(2)).isEqualTo(new Object[] {"james", "US"});

    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables)
        .contains(
            new Object[] {"default", paimontb1, false}, new Object[] {"default", paimontb2, false});

    sql("DROP TABLE %s", paimontb1);
    sql("DROP TABLE %s", paimontb2);
    tables = sql("SHOW TABLES");
    assertThat(tables).doesNotContain(new Object[] {"default", paimontb1, false});
    assertThat(tables).doesNotContain(new Object[] {"default", paimontb2, false});
  }

  @Test
  public void testAlterOperations() {
    String paimontb = getTableNameWithRandomSuffix();
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", paimontb);
    sql("INSERT INTO %s VALUES (1, 'anna'), (2, 'bob')", paimontb);

    sql("ALTER TABLE %s ADD COLUMNS (city STRING, age INT)", paimontb);
    sql("INSERT INTO %s VALUES (3, 'john', 'SFO', 20)", paimontb);

    List<Object[]> results = sql("SELECT * FROM %s ORDER BY id", paimontb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).contains(new Object[] {1, "anna", null, null});
    assertThat(results).contains(new Object[] {2, "bob", null, null});
    assertThat(results).contains(new Object[] {3, "john", "SFO", 20});

    sql("ALTER TABLE %s DROP COLUMN age", paimontb);
    results = sql("SELECT * FROM %s ORDER BY id", paimontb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).contains(new Object[] {1, "anna", null});
    assertThat(results).contains(new Object[] {2, "bob", null});
    assertThat(results).contains(new Object[] {3, "john", "SFO"});

    sql("ALTER TABLE %s RENAME COLUMN city TO address", paimontb);
    results = sql("SELECT id, address FROM %s ORDER BY id", paimontb);
    assertThat(results.size()).isEqualTo(3);
    assertThat(results).contains(new Object[] {1, null});
    assertThat(results).contains(new Object[] {2, null});
    assertThat(results).contains(new Object[] {3, "SFO"});

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('description' = 'people table', 'test-owner' = 'test-user')",
        paimontb);
    List<Object[]> tableInfo = sql("DESCRIBE TABLE EXTENDED %s", paimontb);
    assertThat(tableInfo.stream().map(info -> info[1]).map(String::valueOf))
        .anyMatch(info -> info.contains("description") && info.contains("test-owner"));

    sql("DROP TABLE %s", paimontb);
  }

  @Test
  public void testUnsupportedAlterTableOperations() {
    String paimontb = getTableNameWithRandomSuffix();
    sql(
        "CREATE TABLE %s (name STRING, age INT, country STRING) USING PAIMON PARTITIONED BY (country)",
        paimontb);

    assertThatThrownBy(() -> sql("ALTER TABLE %s RENAME TO new_paimon", paimontb))
        .isInstanceOf(UnsupportedOperationException.class);

    sql("DROP TABLE %s", paimontb);
  }

  @Test
  public void testTableCreateOperations() {
    String paimontb = getTableNameWithRandomSuffix();
    sql("CREATE TABLE %s (id INT, name STRING) USING PAIMON", paimontb);

    List<Object[]> tables = sql("SHOW TABLES");
    assertThat(tables).contains(new Object[] {"default", paimontb, false});
    sql("DROP TABLE %s", paimontb);

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s USING PAIMON AS SELECT 1 AS id",
                    getTableNameWithRandomSuffix()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static boolean classAvailable(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
