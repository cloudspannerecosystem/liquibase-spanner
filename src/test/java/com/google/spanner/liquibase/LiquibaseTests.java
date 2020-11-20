/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.spanner.liquibase;

import static com.google.common.truth.Truth.assertThat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// As these are forms of integration tests -- against an external emulator or a real Spanner
// instance --
// they must run serially to reduce contention and increase test reliability.
@Execution(ExecutionMode.SAME_THREAD)
public class LiquibaseTests {
  private static final Logger logger = LoggerFactory.getLogger(LiquibaseTests.class);

  // Return spannerReal instance
  // It is shared across tests.
  private static TestHarness.Connection spannerReal;

  static TestHarness.Connection getSpannerReal() throws SQLException {
    if (spannerReal == null) {
      spannerReal = TestHarness.useSpannerConnection();
    }
    return spannerReal;
  }

  // Return spannerEmulator instance
  // It is shared across tests.
  private static TestHarness.Connection spannerEmulator;

  static TestHarness.Connection getSpannerEmulator() throws SQLException {
    if (spannerEmulator == null) {
      spannerEmulator = TestHarness.useSpannerEmulator();
    }
    return spannerEmulator;
  }

  // Stop all instances and do cleanup if necessary
  @AfterAll
  static void stopTestHarness() throws SQLException {
    if (spannerReal != null) {
      spannerReal.stop();
    }
    if (spannerEmulator != null) {
      spannerEmulator.stop();
    }
  }

  // Get the Liquibase changeset for the given log file and JDBC
  Liquibase getLiquibase(TestHarness.Connection testHarness, String changeLogFile)
      throws DatabaseException {
    Liquibase liquibase =
        new Liquibase(
            changeLogFile,
            new ClassLoaderResourceAccessor(),
            DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(
                    new JdbcConnection(testHarness.getJDBCConnection())));

    return liquibase;
  }

  @Test
  void doSpannerEmulatorSanityCheckTest() throws SQLException, LiquibaseException {
    doSanityCheckTest(getSpannerEmulator());
  }

  @Test
  @Tag("integration")
  void doSpannerRealSanityCheckTest() throws SQLException, LiquibaseException {
    doSanityCheckTest(getSpannerReal());
  }

  void doSanityCheckTest(TestHarness.Connection liquibaseTestHarness) throws SQLException {

    // Execute Query
    List<Map<String, Object>> rss = testQuery(liquibaseTestHarness.getJDBCConnection(), "SELECT 3");

    // Validate results
    Assert.assertTrue(rss.size() == 1);
    Assert.assertTrue(rss.get(0).get("1").equals("3"));

    // Ensure SELECT COUNT(*) FROM DATABASECHANGELOGLOCK works
    rss =
        testQuery(
            liquibaseTestHarness.getJDBCConnection(), "SELECT COUNT(*) FROM DATABASECHANGELOGLOCK");

    Assert.assertTrue("DATABASECHANGELOGLOCK does not exist", rss.size() == 1);
  }

  @Test
  void doSpannerUpdateEmulatorTest() throws SQLException, LiquibaseException {
    doLiquibaseUpdateTest(getSpannerEmulator());
  }

  @Test
  @Tag("integration")
  void doSpannerUpdateRealTest() throws SQLException, LiquibaseException {
    doLiquibaseUpdateTest(getSpannerReal());
  }

  void doLiquibaseUpdateTest(TestHarness.Connection testHarness)
      throws SQLException, LiquibaseException {
    Liquibase liquibase = getLiquibase(testHarness, "changelog.spanner.sql");

    liquibase.update(null, new LabelExpression("base"));
    liquibase.tag("mytag");

    List<Map<String, Object>> rss =
        testQuery(testHarness.getJDBCConnection(), "SELECT id, name, extra FROM table1");
    Assert.assertTrue(rss.size() == 1);
    Assert.assertTrue(rss.get(0).get("id").equals("1"));
    Assert.assertTrue(rss.get(0).get("name").equals("test"));
    Assert.assertTrue(rss.get(0).get("extra").equals("abc"));

    rss = testQuery(testHarness.getJDBCConnection(), "SELECT COUNT(*) FROM DATABASECHANGELOG");
    Assert.assertTrue(rss.size() == 1);
    Assert.assertTrue(rss.get(0).get("1").equals("3"));
  }

  @Test
  void doEmulatorSpannerCreateAndRollbackTest() throws SQLException, LiquibaseException {
    doLiquibaseCreateAndRollbackTest("create_table.spanner.sql", getSpannerEmulator());
    doLiquibaseCreateAndRollbackTest("create_table.spanner.yaml", getSpannerEmulator());
  }

  @Test
  @Tag("integration")
  void doRealSpannerCreateAndRollbackTest() throws SQLException, LiquibaseException {
    doLiquibaseCreateAndRollbackTest("create_table.spanner.sql", getSpannerReal());
    doLiquibaseCreateAndRollbackTest("create_table.spanner.yaml", getSpannerReal());
  }

  void doLiquibaseCreateAndRollbackTest(String changeLogFile, TestHarness.Connection testHarness)
      throws SQLException, LiquibaseException {

    testTableColumns(testHarness.getJDBCConnection(), "rollback_table");


    Liquibase liquibase = getLiquibase(testHarness, changeLogFile);
    liquibase.update(null, new LabelExpression("rollback_stuff"));
    liquibase.tag("tag-at-rollback");

    testTableColumns(testHarness.getJDBCConnection(), "rollback_table",
        new ColDesc("id", "INT64", Boolean.FALSE),
        new ColDesc("name", "STRING(255)")
    );

    testTablePrimaryKeys(testHarness.getJDBCConnection(), "rollback_table",
        new ColDesc("id"));

    // Do rollback
    liquibase.rollback(1, null);

    // Ensure nothing is there!
    testTableColumns(testHarness.getJDBCConnection(), "rollback_table");
  }

  @Test
  void doEmulatorSpannerCreateAllDataTypesTest()
      throws SQLException, LiquibaseException {

    // Emulator only -- we need an exception here to skip the NUMERIC type as this is not
    // yet supported by the emulator.
    TestHarness.Connection testHarness = getSpannerEmulator();

    // No columns yet in the table -- it doesn't exist
    testTableColumns(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes");

    // Run the Liquibase
    Liquibase liquibase = getLiquibase(testHarness,
        "create-table-with-all-liquibase-types-except-decimal.spanner.yaml");
    liquibase.update(null, new LabelExpression("version 0.3"));
    liquibase.tag("tag-at-rollback_all_types");

    // Expect all of the columns and types
    testTableColumns(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes",
        new ColDesc("ColBigInt", "INT64", Boolean.FALSE),
        new ColDesc("ColBlob", "BYTES(MAX)"),
        new ColDesc("ColBoolean", "BOOL"),
        new ColDesc("ColChar", "STRING(100)"),
        new ColDesc("ColNChar", "STRING(50)"),
        new ColDesc("ColNVarchar", "STRING(100)"),
        new ColDesc("ColVarchar", "STRING(200)"),
        new ColDesc("ColClob", "STRING(MAX)"),
        new ColDesc("ColDateTime", "TIMESTAMP"),
        new ColDesc("ColTimestamp", "TIMESTAMP"),
        new ColDesc("ColDate", "DATE"),
        new ColDesc("ColDouble", "FLOAT64"),
        new ColDesc("ColFloat", "FLOAT64"),
        new ColDesc("ColInt", "INT64"),
        new ColDesc("ColMediumInt", "INT64"),
        new ColDesc("ColSmallInt", "INT64"),
        new ColDesc("ColTime", "TIMESTAMP"),
        new ColDesc("ColTinyInt", "INT64"),
        new ColDesc("ColUUID", "STRING(36)"),
        new ColDesc("ColXml", "STRING(MAX)")
        );

    testTablePrimaryKeys(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes",
        new ColDesc("ColBigInt"));

    // Do rollback
    liquibase.rollback(1, null);

    // Ensure nothing is there!
    testTableColumns(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes");
  }

  @Test
  @Tag("integration")
  void doRealSpannerCreateAllDataTypesTest()
      throws SQLException, LiquibaseException {

    // Real Spanner -- test all supported tests
    TestHarness.Connection testHarness = getSpannerReal();

    // No columns yet in the table -- it doesn't exist
    testTableColumns(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes");

    // Run the Liquibase with all types
    Liquibase liquibase = getLiquibase(testHarness,
        "create-table-with-all-liquibase-types.spanner.yaml");
    liquibase.update(null, new LabelExpression("version 0.3"));
    liquibase.tag("tag-at-rollback_all_types");

    // Expect all of the columns and types
    testTableColumns(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes",
        new ColDesc("ColBigInt", "INT64", Boolean.FALSE),
        new ColDesc("ColBlob", "BYTES(MAX)"),
        new ColDesc("ColBoolean", "BOOL"),
        new ColDesc("ColChar", "STRING(100)"),
        new ColDesc("ColNChar", "STRING(50)"),
        new ColDesc("ColNVarchar", "STRING(100)"),
        new ColDesc("ColVarchar", "STRING(200)"),
        new ColDesc("ColClob", "STRING(MAX)"),
        new ColDesc("ColDateTime", "TIMESTAMP"),
        new ColDesc("ColTimestamp", "TIMESTAMP"),
        new ColDesc("ColDate", "DATE"),
        new ColDesc("ColDecimal", "NUMERIC"),
        new ColDesc("ColDouble", "FLOAT64"),
        new ColDesc("ColFloat", "FLOAT64"),
        new ColDesc("ColInt", "INT64"),
        new ColDesc("ColMediumInt", "INT64"),
        new ColDesc("ColNumber", "NUMERIC"),
        new ColDesc("ColSmallInt", "INT64"),
        new ColDesc("ColTime", "TIMESTAMP"),
        new ColDesc("ColTinyInt", "INT64"),
        new ColDesc("ColUUID", "STRING(36)"),
        new ColDesc("ColXml", "STRING(MAX)")
    );

    testTablePrimaryKeys(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes",
        new ColDesc("ColBigInt"));

    // Do rollback
    liquibase.rollback(1, null);

    // Ensure nothing is there!
    testTableColumns(testHarness.getJDBCConnection(), "TableWithAllLiquibaseTypes");
  }

  public static class ColDesc {
    public final String name;
    public final String type;
    public final Boolean isNullable;
    public ColDesc(String name) {
      this(name, null, Boolean.TRUE);
    }
    public ColDesc(String name, String type) {
      this(name, type, Boolean.TRUE);
    }
    public ColDesc(String name, String type, Boolean isNullable) {
      this.name = name;
      this.type = type;
      this.isNullable = isNullable;
    }
  }

  static void testTableColumns(java.sql.Connection conn, String table, ColDesc... cols)
      throws SQLException {

    boolean readOnlyStatus = conn.isReadOnly();
    conn.setReadOnly(true);
    ResultSet rs = conn.getMetaData().getColumns(null, null, table, null);
    conn.setReadOnly(readOnlyStatus);

    List<Map<String, Object>> rows = getResults(rs);

    Assert.assertEquals(rows.size(), cols.length);
    for (int i = 0; i < cols.length; i++) {
      Assert.assertEquals(
          rows.get(i).get("COLUMN_NAME").toString().compareTo(cols[i].name),
          0);
      if (cols[i].type != null) {
        Assert.assertEquals(
            rows.get(i).get("TYPE_NAME").toString().compareToIgnoreCase(cols[i].type),
            0);
      }
      if (cols[i].isNullable != null) {
        String expectedValue = cols[i].isNullable ? "YES" : "NO";
        Assert.assertEquals(
            rows.get(i).get("IS_NULLABLE").toString().compareToIgnoreCase(expectedValue),
            0);
      }
    }
  }

  static void testTablePrimaryKeys(java.sql.Connection conn, String table, ColDesc... cols)
      throws SQLException {

    boolean readOnlyStatus = conn.isReadOnly();
    conn.setReadOnly(true);
    ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, table);
    conn.setReadOnly(readOnlyStatus);

    List<Map<String, Object>> rows = getResults(rs);

    Assert.assertEquals(rows.size(), cols.length);
    for (int i = 0; i < cols.length; i++) {
      Assert.assertEquals(
          rows.get(i).get("COLUMN_NAME").toString().compareTo(cols[i].name),
          0);
    }
  }

  static List<Map<String, Object>> testQuery(java.sql.Connection conn, String query)
      throws SQLException {

    // Set readonly
    boolean readOnlyStatus = conn.isReadOnly();
    conn.setReadOnly(true);
    ResultSet rs = conn.createStatement().executeQuery(query);
    logger.info(String.format("Query: %s, results: %s", query, rs.toString()));
    conn.setReadOnly(readOnlyStatus);
    return getResults(rs);
  }

  static List<Map<String, Object>> getResults(ResultSet rs) throws SQLException {
    ArrayList<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    while (rs.next()) {
      Map rowVals = new HashMap<String, Object>();
      for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
        String columnName = rs.getMetaData().getColumnName(i);
        if (columnName.equals("")) {
          columnName = String.format("%d", i);
        }
        String value = null;
        if (rs.getObject(i) != null) {
          value = rs.getObject(i).toString();
        }
        rowVals.put(columnName, value);
        logger.info(String.format("Row %d, column %s = %s", results.size(), columnName, value));
      }
      results.add(rowVals);
    }

    return results;
  }

  @Test
  void doEmulatorSpannerSetNullableTest()
      throws SQLException, LiquibaseException {

    TestHarness.Connection testHarness = getSpannerEmulator();
    // Create a simple table.
    testHarness.getJDBCConnection().createStatement().execute(
          "CREATE TABLE Singers (\n"
        + "  SingerId INT64 NOT NULL,\n"
        + "  LastName STRING(100),\n"
        + ") PRIMARY KEY (SingerId)");
    assertThat(isNullable(testHarness.getJDBCConnection(), "Singers", "LastName")).isTrue();

    // Run a Liquibase update to make the LastName column NOT NULL.
    Liquibase makeNotNull = getLiquibase(testHarness,
        "add-not-null-constraint-singers-lastname.spanner.yaml");
    makeNotNull.update(new Contexts("test"));
    assertThat(isNullable(testHarness.getJDBCConnection(), "Singers", "LastName")).isFalse();

    // Do rollback.
    makeNotNull.rollback(1, "test");
    assertThat(isNullable(testHarness.getJDBCConnection(), "Singers", "LastName")).isTrue();
    
    // Manually make the column NOT NULL.
    testHarness.getJDBCConnection().createStatement().execute("ALTER TABLE Singers ALTER COLUMN LastName STRING(100) NOT NULL");
    assertThat(isNullable(testHarness.getJDBCConnection(), "Singers", "LastName")).isFalse();
    
    Liquibase makeNullable = getLiquibase(testHarness,
        "drop-not-null-constraint-singers-lastname.spanner.yaml");
    makeNullable.update(new Contexts("test"));
    assertThat(isNullable(testHarness.getJDBCConnection(), "Singers", "LastName")).isTrue();

    // Do rollback.
    makeNullable.rollback(1, "test");
    assertThat(isNullable(testHarness.getJDBCConnection(), "Singers", "LastName")).isFalse();
    
    testHarness.getJDBCConnection().createStatement().execute("DROP TABLE Singers");
  }
  
  static boolean isNullable(java.sql.Connection conn, String table, String column) throws SQLException {
    boolean readOnlyStatus = conn.isReadOnly();
    conn.setReadOnly(true);
    try (ResultSet rs = conn.getMetaData().getColumns(null, null, table, column)) {
      while (rs.next()) {
        return "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
      }
    } finally {
      conn.setReadOnly(readOnlyStatus);
    }
    return false;
  }
}
