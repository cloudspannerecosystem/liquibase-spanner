/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.spanner.liquibase;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import liquibase.CatalogAndSchema;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.integration.commandline.LiquibaseCommandLine;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

// As these are forms of integration tests -- against an external emulator or a real Spanner
// instance --
// they must run serially to reduce contention and increase test reliability.
@Execution(ExecutionMode.SAME_THREAD)
public class LiquibaseTests {
  private static final Logger logger = LoggerFactory.getLogger(LiquibaseTests.class);

  // Return spannerReal instance
  // It is shared across tests.
  private static TestHarness.Connection spannerReal;
  private Dialect dialect = Dialect.GOOGLE_STANDARD_SQL;

  static TestHarness.Connection getSpannerReal(Dialect dialect) throws SQLException {
    spannerReal = TestHarness.useSpannerConnection(dialect);
    return spannerReal;
  }

  // Return spannerEmulator instance
  // It is shared across tests.
  private static TestHarness.Connection spannerEmulator;

  static TestHarness.Connection getSpannerEmulator(Dialect dialect) throws SQLException {
    spannerEmulator = TestHarness.useSpannerEmulator(dialect);
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
      throws DatabaseException, SQLException {
    Liquibase liquibase =
        new Liquibase(
            changeLogFile,
            new ClassLoaderResourceAccessor(),
            DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(
                    new JdbcConnection(
                        DriverManager.getConnection(testHarness.getConnectionUrl()))));

    return liquibase;
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doSpannerEmulatorSanityCheckTest(Dialect dialect) throws SQLException, LiquibaseException {
    this.dialect = dialect;
    doSanityCheckTest(getSpannerEmulator(dialect));
  }

  @Tag("integration")
  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doSpannerRealSanityCheckTest(Dialect dialect) throws SQLException, LiquibaseException {
    doSanityCheckTest(getSpannerReal(dialect));
  }

  void doSanityCheckTest(TestHarness.Connection liquibaseTestHarness) throws SQLException {

    try (Connection connection =
        DriverManager.getConnection(liquibaseTestHarness.getConnectionUrl())) {
      // Execute Query
      List<Map<String, Object>> rss = testQuery(connection, "SELECT 3 as test");

      // Validate results
      Assert.assertTrue(rss.size() == 1);
      Assert.assertTrue(rss.get(0).get("test").equals("3"));

      // Ensure SELECT COUNT(*) FROM DATABASECHANGELOGLOCK works
      rss = testQuery(connection, "SELECT COUNT(*) FROM DATABASECHANGELOGLOCK");

      Assert.assertTrue("DATABASECHANGELOGLOCK does not exist", rss.size() == 1);
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doSpannerUpdateEmulatorTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doLiquibaseUpdateTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doSpannerUpdateRealTest() throws Exception {
    doLiquibaseUpdateTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorDropAllForeignKeysTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    logger.warn("Starting emulator foreign key test");
    doDropAllForeignKeysTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerDropAllForeignKeysTest() throws Exception {
    doDropAllForeignKeysTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doDropAllForeignKeysTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Countries (Code varchar(10), Name varchar(100), PRIMARY KEY (Code))"
                : "CREATE TABLE Countries (Code STRING(10), Name STRING(100)) PRIMARY KEY (Code)");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers (SingerId bigint, Name varchar(100), Country varchar(100), CONSTRAINT FK_Singers_Countries FOREIGN KEY (Country) REFERENCES Countries (Code), PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers (SingerId INT64, Name STRING(100), Country STRING(100), CONSTRAINT FK_Singers_Countries FOREIGN KEY (Country) REFERENCES Countries (Code)) PRIMARY KEY (SingerId)");
        statement.execute("RUN BATCH");

        try (ResultSet rs =
            statement.executeQuery(
                dialect == Dialect.POSTGRESQL
                    ? "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME='fk_singers_countries'"
                    : "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME='FK_Singers_Countries'")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getLong(1)).isEqualTo(1L);
          assertThat(rs.next()).isFalse();
        }

        try (Liquibase liquibase =
            getLiquibase(
                testHarness,
                dialect == Dialect.POSTGRESQL
                    ? "drop-all-foreign-key-constraints-singers.pg.spanner.yaml"
                    : "drop-all-foreign-key-constraints-singers.spanner.yaml")) {
          liquibase.update(new Contexts("test"));

          try (ResultSet rs =
              statement.executeQuery(
                  dialect == Dialect.POSTGRESQL
                      ? "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME='fk_singers_countries'"
                      : "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME='FK_Singers_Countries'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(0L);
            assertThat(rs.next()).isFalse();
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP TABLE Singers");
          statement.execute("DROP TABLE Countries");
          statement.execute("RUN BATCH");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorMergeColumnsTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doMergeColumnsTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerMergeColumnsTest() throws Exception {
    doMergeColumnsTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doMergeColumnsTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers (SingerId bigint, FirstName varchar(100), LastName varchar(100), PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers (SingerId INT64, FirstName STRING(100), LastName STRING(100)) PRIMARY KEY (SingerId)");
        statement.execute("RUN BATCH");

        Object[][] singers =
            new Object[][] {
              {1L, "FirstName1", "LastName1"},
              {2L, "FirstName2", "LastName2"},
              {3L, "FirstName3", "LastName3"},
            };
        statement.execute("BEGIN");
        try (PreparedStatement ps =
            con.prepareStatement(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (?, ?, ?)")) {
          for (Object[] singer : singers) {
            for (int p = 0; p < singer.length; p++) {
              // JDBC param index is 1-based.
              ps.setObject(p + 1, singer[p]);
            }
            ps.addBatch();
          }
          ps.executeBatch();
        }
        statement.execute("COMMIT");

        try {
          Liquibase liquibase =
              getLiquibase(testHarness, "merge-singers-firstname-and-lastname.spanner.yaml");
          liquibase.update(new Contexts("test"));

          try (ResultSet rs = statement.executeQuery("SELECT * FROM Singers ORDER BY SingerId")) {
            for (int i = 1; i <= singers.length; i++) {
              assertThat(rs.next()).isTrue();
              assertThat(rs.getString(dialect == Dialect.POSTGRESQL ? "fullname" : "FullName"))
                  .isEqualTo(String.format("FirstName%dsome-stringLastName%d", i, i));
            }
            assertThat(rs.next()).isFalse();
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP TABLE Singers");
          statement.execute("RUN BATCH");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorModifyDataTypeTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doModifyDataTypeTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerModifyDataTypeTest() throws Exception {
    doModifyDataTypeTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doModifyDataTypeTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers (SingerId bigint, LastName varchar(100) NOT NULL, SingerInfo bytea, PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers (SingerId INT64, LastName STRING(100) NOT NULL, SingerInfo BYTES(MAX)) PRIMARY KEY (SingerId)");
        statement.execute("RUN BATCH");

        try (ResultSet rs =
            statement.executeQuery(
                dialect == Dialect.POSTGRESQL
                    ? "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='singers' AND COLUMN_NAME='lastname'"
                    : "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='Singers' AND COLUMN_NAME='LastName'")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1))
              .isEqualTo(dialect == Dialect.POSTGRESQL ? "character varying(100)" : "STRING(100)");
          assertThat(rs.getString(2)).isEqualTo("NO");
          assertThat(rs.next()).isFalse();
        }
        try (ResultSet rs =
            statement.executeQuery(
                dialect == Dialect.POSTGRESQL
                    ? "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='singers' AND COLUMN_NAME='singerinfo'"
                    : "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='Singers' AND COLUMN_NAME='SingerInfo'")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1))
              .isEqualTo(dialect == Dialect.POSTGRESQL ? "bytea" : "BYTES(MAX)");
          assertThat(rs.getString(2)).isEqualTo("YES");
          assertThat(rs.next()).isFalse();
        }

        try (Liquibase liquibase =
            getLiquibase(
                testHarness,
                dialect == Dialect.POSTGRESQL
                    ? "modify-data-type-singers-lastname.pg.spanner.yaml"
                    : "modify-data-type-singers-lastname.spanner.yaml")) {
          liquibase.update(new Contexts("test"));

          try (ResultSet rs =
              statement.executeQuery(
                  dialect == Dialect.POSTGRESQL
                      ? "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='singers' AND COLUMN_NAME='lastname'"
                      : "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='Singers' AND COLUMN_NAME='LastName'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1))
                .isEqualTo(
                    dialect == Dialect.POSTGRESQL ? "character varying(1000)" : "STRING(1000)");
            assertThat(rs.getString(2)).isEqualTo("NO");
            assertThat(rs.next()).isFalse();
          }
        }

        try (Liquibase liquibase =
            getLiquibase(
                testHarness,
                dialect == Dialect.POSTGRESQL
                    ? "modify-data-type-singers-singerinfo.pg.spanner.yaml"
                    : "modify-data-type-singers-singerinfo.spanner.yaml")) {
          liquibase.update(new Contexts("test"));
          try (ResultSet rs =
              statement.executeQuery(
                  dialect == Dialect.POSTGRESQL
                      ? "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='singers' AND COLUMN_NAME='singerinfo'"
                      : "SELECT SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='Singers' AND COLUMN_NAME='SingerInfo'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1))
                .isEqualTo(dialect == Dialect.POSTGRESQL ? "character varying" : "STRING(MAX)");
            assertThat(rs.getString(2)).isEqualTo("YES");
            assertThat(rs.next()).isFalse();
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP TABLE Singers");
          statement.execute("RUN BATCH");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorLoadDataTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doLoadDataTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerLoadDataTest() throws Exception {
    doLoadDataTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doLoadDataTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers ("
                    + "SingerId bigint,"
                    + "Name varchar(100),"
                    + "Description varchar,"
                    + "SingerInfo bytea,"
                    + "AnyGood boolean,"
                    + "Birthdate date,"
                    + "LastConcertTimestamp timestamptz,"
                    + "ExternalID varchar(36),"
                    + "PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers ("
                    + "SingerId INT64,"
                    + "Name STRING(100),"
                    + "Description STRING(MAX),"
                    + "SingerInfo BYTES(MAX),"
                    + "AnyGood BOOL,"
                    + "Birthdate DATE,"
                    + "LastConcertTimestamp TIMESTAMP,"
                    + "ExternalID STRING(36),"
                    + ") PRIMARY KEY (SingerId)");
        statement.execute("RUN BATCH");
        try (Liquibase liquibase = getLiquibase(testHarness, "load-data-singers.spanner.yaml")) {
          liquibase.update(new Contexts("test"));

          try (ResultSet rs = statement.executeQuery("SELECT * FROM Singers ORDER BY SingerId")) {
            int index = 0;
            while (rs.next()) {
              index++;
              assertThat(rs.getLong(dialect == Dialect.POSTGRESQL ? "singerid" : "SingerId"))
                  .isEqualTo(index);
              assertThat(rs.getString(dialect == Dialect.POSTGRESQL ? "name" : "Name"))
                  .ignoringCase()
                  .isEqualTo("Name " + index);
              assertThat(
                      rs.getString(dialect == Dialect.POSTGRESQL ? "description" : "Description"))
                  .isEqualTo("This is a CLOB description " + index);
              assertThat(rs.getBytes(dialect == Dialect.POSTGRESQL ? "singerinfo" : "SingerInfo"))
                  .isEqualTo(ByteArray.copyFrom("singerinfo " + index).toByteArray());
              assertThat(rs.getBoolean(dialect == Dialect.POSTGRESQL ? "anygood" : "AnyGood"))
                  .isEqualTo(index % 2 == 0);
            }
            assertThat(index).isEqualTo(3);
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP TABLE Singers");
          statement.execute("RUN BATCH");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorLoadDataWithSingleQuotesTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doLoadDataWithSingleQuotesTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerLoadDataWithSingleQuotesTest() throws Exception {
    doLoadDataWithSingleQuotesTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doLoadDataWithSingleQuotesTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE TableWithEscapedStringData ("
                    + "Id bigint,"
                    + "ColString varchar(100),"
                    + "PRIMARY KEY (Id))"
                : "CREATE TABLE TableWithEscapedStringData ("
                    + "Id INT64,"
                    + "ColString STRING(100),"
                    + ") PRIMARY KEY (Id)");
        statement.execute("RUN BATCH");
        try (Liquibase liquibase =
            getLiquibase(testHarness, "load-data-with-single-quotes.spanner.yaml")) {
          liquibase.update(new Contexts("test"));

          try (ResultSet rs =
              statement.executeQuery("SELECT * FROM TableWithEscapedStringData ORDER BY Id")) {
            int index = 0;
            while (rs.next()) {
              index++;
              assertThat(rs.getLong(dialect == Dialect.POSTGRESQL ? "id" : "Id")).isEqualTo(index);
              assertThat(rs.getString(dialect == Dialect.POSTGRESQL ? "colstring" : "ColString"))
                  .isEqualTo("Shouldn't have an issue inserting this as row " + index);
            }
            assertThat(index).isEqualTo(3);
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP TABLE TableWithEscapedStringData");
          statement.execute("RUN BATCH");
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorLoadUpdateDataTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doLoadUpdateDataTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerLoadUpdateDataTest() throws Exception {
    doLoadUpdateDataTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doLoadUpdateDataTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers ("
                    + "SingerId bigint,"
                    + "Name varchar(100),"
                    + "Description varchar,"
                    + "SingerInfo bytea,"
                    + "AnyGood boolean,"
                    + "Birthdate date,"
                    + "LastConcertTimestamp timestamptz,"
                    + "ExternalID varchar(36),"
                    + "PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers ("
                    + "SingerId INT64,"
                    + "Name STRING(100),"
                    + "Description STRING(MAX),"
                    + "SingerInfo BYTES(MAX),"
                    + "AnyGood BOOL,"
                    + "Birthdate DATE,"
                    + "LastConcertTimestamp TIMESTAMP,"
                    + "ExternalID STRING(36),"
                    + ") PRIMARY KEY (SingerId)");
        statement.execute("RUN BATCH");
        // Insert one record that will be updated by Liquibase.
        CloudSpannerJdbcConnection cs = con.unwrap(CloudSpannerJdbcConnection.class);
        boolean wasAutoCommit = cs.getAutoCommit();
        cs.setAutoCommit(true);
        cs.write(
            Mutation.newInsertBuilder("Singers")
                .set("SingerId")
                .to(2L)
                .set("Name")
                .to("Some initial name")
                .set("Description")
                .to("Some initial description")
                .set("SingerInfo")
                .to(ByteArray.copyFrom("Some initial singer info"))
                .set("AnyGood")
                .to(false)
                .set("Birthdate")
                .to(Date.fromYearMonthDay(2000, 1, 1))
                .set("LastConcertTimestamp")
                .to(Timestamp.ofTimeMicroseconds(12345678))
                .set("ExternalId")
                .to("some initial external id")
                .build());
        cs.setAutoCommit(wasAutoCommit);
        try (Liquibase liquibase =
            getLiquibase(testHarness, "load-update-data-singers.spanner.yaml")) {
          liquibase.update(new Contexts("test"));

          try (ResultSet rs = statement.executeQuery("SELECT * FROM Singers ORDER BY SingerId")) {
            int index = 0;
            while (rs.next()) {
              index++;
              assertThat(rs.getLong(dialect == Dialect.POSTGRESQL ? "singerid" : "SingerId"))
                  .isEqualTo(index);
              assertThat(rs.getString(dialect == Dialect.POSTGRESQL ? "name" : "Name"))
                  .isEqualTo("Name " + index);
              assertThat(
                      rs.getString(dialect == Dialect.POSTGRESQL ? "description" : "Description"))
                  .isEqualTo("Description " + index);
              assertThat(rs.getBytes(dialect == Dialect.POSTGRESQL ? "singerinfo" : "SingerInfo"))
                  .isNull();
              assertThat(rs.getBoolean(dialect == Dialect.POSTGRESQL ? "anygood" : "AnyGood"))
                  .isEqualTo(index % 2 == 0);
            }
            assertThat(index).isEqualTo(3);
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP TABLE Singers");
          statement.execute("RUN BATCH");
        }
      }
    }
  }

  void doLiquibaseUpdateTest(TestHarness.Connection testHarness) throws Exception {
    String changelogFile =
        dialect == Dialect.POSTGRESQL ? "changelog.spanner.pg.sql" : "changelog.spanner.sql";

    try (Liquibase liquibase = getLiquibase(testHarness, changelogFile)) {
      liquibase.update(null, new LabelExpression("base"));
      liquibase.tag("mytag");

      Connection currentConnection =
          ((JdbcConnection) liquibase.getDatabase().getConnection()).getUnderlyingConnection();

      List<Map<String, Object>> rss =
          testQuery(currentConnection, "SELECT id, name, extra FROM table1");
      List<Map<String, Object>> rss2 =
          testQuery(currentConnection, "SELECT * FROM DATABASECHANGELOG");

      Assert.assertTrue(rss.size() == 1);
      Assert.assertTrue(rss.get(0).get("id").equals("1"));
      Assert.assertTrue(rss.get(0).get("name").equals("test"));
      Assert.assertTrue(rss.get(0).get("extra").equals("abc"));

      rss = testQuery(currentConnection, "SELECT COUNT(*) as count FROM DATABASECHANGELOG");
      Assert.assertTrue(rss.size() == 1);
      Assert.assertTrue(rss.get(0).get("count").equals("3"));
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorCreateSequenceTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doLiquibaseCreateSequenceTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerCreateSequenceTest() throws Exception {
    doLiquibaseCreateSequenceTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doLiquibaseCreateSequenceTest(TestHarness.Connection testHarness) throws Exception {
    try (Liquibase liquibase = getLiquibase(testHarness, "create-sequence.spanner.yaml")) {
      // Verify that there is no sequence in the database.
      String catalog = liquibase.getDatabase().getDefaultCatalogName();
      String schema = liquibase.getDatabase().getDefaultSchemaName();
      String sql =
          dialect == Dialect.POSTGRESQL
              ? String.format(
                  "select sequence_name from information_schema.sequences where sequence_catalog='%s' and sequence_schema='%s' and sequence_name='idsequence'",
                  catalog, schema)
              : "select name from information_schema.sequences where catalog='' and schema='' and name='IdSequence'";
      Connection connection =
          ((JdbcConnection) liquibase.getDatabase().getConnection()).getUnderlyingConnection();
      try (ResultSet sequences = connection.createStatement().executeQuery(sql)) {
        assertThat(sequences.next()).isFalse();
      }

      // Run a change set that creates a sequence.
      liquibase.update(null, new LabelExpression("test-create-sequence"));

      // Verify that the sequence was created.
      try (ResultSet sequences = connection.createStatement().executeQuery(sql)) {
        assertThat(sequences.next()).isTrue();
        assertThat(sequences.getString(1))
            .isEqualTo(dialect == Dialect.POSTGRESQL ? "idsequence" : "IdSequence");
        assertThat(sequences.next()).isFalse();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorSpannerCreateAndRollbackTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doLiquibaseCreateAndRollbackTest(
        dialect == Dialect.POSTGRESQL ? "create_table.spanner.pg.sql" : "create_table.spanner.sql",
        getSpannerEmulator(dialect));
    doLiquibaseCreateAndRollbackTest("create_table.spanner.yaml", getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerCreateAndRollbackTest() throws Exception {
    doLiquibaseCreateAndRollbackTest(
        "create_table.spanner.sql", getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
    doLiquibaseCreateAndRollbackTest(
        "create_table.spanner.yaml", getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doLiquibaseCreateAndRollbackTest(String changeLogFile, TestHarness.Connection testHarness)
      throws Exception {
    try (Connection connection = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      testTableColumns(dialect, connection, "rollback_table");
    }

    try (Liquibase liquibase = getLiquibase(testHarness, changeLogFile)) {
      liquibase.update(null, new LabelExpression("rollback_stuff"));
      liquibase.tag("tag-at-rollback");

      Connection currentConnection =
          ((JdbcConnection) liquibase.getDatabase().getConnection()).getUnderlyingConnection();
      if (this.dialect == Dialect.POSTGRESQL) {
        testTableColumns(
            dialect,
            currentConnection,
            "rollback_table",
            new ColDesc("id", "bigint", Boolean.FALSE),
            new ColDesc("name", "varchar(255)"));
      } else {
        testTableColumns(
            dialect,
            currentConnection,
            "rollback_table",
            new ColDesc("id", "INT64", Boolean.FALSE),
            new ColDesc("name", "STRING(255)"));
      }

      testTablePrimaryKeys(currentConnection, "rollback_table", new ColDesc("id"));

      // Do rollback
      liquibase.rollback(1, null);

      // Ensure nothing is there!
      testTableColumns(dialect, currentConnection, "rollback_table");
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorSpannerCreateAllDataTypesTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doSpannerCreateAllDataTypesTest(getSpannerEmulator(dialect));
  }

  @Tag("integration")
  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doRealSpannerCreateAllDataTypesTest(Dialect dialect) throws Exception {
    doSpannerCreateAllDataTypesTest(getSpannerReal(dialect));
  }

  private void doSpannerCreateAllDataTypesTest(TestHarness.Connection testHarness)
      throws Exception {
    try (Connection connection = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      // No columns yet in the table -- it doesn't exist
      testTableColumns(dialect, connection, "TableWithAllLiquibaseTypes");
    }

    // Run the Liquibase with all types
    try (Liquibase liquibase =
        getLiquibase(testHarness, "create-table-with-all-liquibase-types.spanner.yaml")) {
      liquibase.update(null, new LabelExpression("version 0.3"));
      liquibase.tag("tag-at-rollback_all_types");

      Connection currentConnection =
          ((JdbcConnection) liquibase.getDatabase().getConnection()).getUnderlyingConnection();
      // Expect all of the columns and types
      ColDesc[] cols =
          dialect == Dialect.POSTGRESQL
              ? new ColDesc[] {
                new ColDesc("colbigint", "bigint", Boolean.FALSE),
                new ColDesc("colblob", "bytea"),
                new ColDesc("colboolean", "boolean"),
                new ColDesc("colchar", "varchar(100)"),
                new ColDesc("colnchar", "varchar(50)"),
                new ColDesc("colnvarchar", "varchar(100)"),
                new ColDesc("colvarchar", "varchar(200)"),
                new ColDesc("colclob", "varchar"),
                new ColDesc("coldatetime", "timestamptz"),
                new ColDesc("coltimestamp", "timestamptz"),
                new ColDesc("coldate", "date"),
                new ColDesc("coldecimal", "numeric"),
                new ColDesc("coldouble", "float8"),
                new ColDesc("colfloat", "real"),
                new ColDesc("colint", "bigint"),
                new ColDesc("colmediumint", "bigint"),
                new ColDesc("colnumber", "numeric"),
                new ColDesc("colsmallint", "bigint"),
                new ColDesc("coltime", "timestamptz"),
                new ColDesc("coltinyint", "bigint"),
                new ColDesc("coluuid", "varchar(36)"),
                new ColDesc("colxml", "varchar"),
                new ColDesc("colboolarray", "boolean[]"),
                new ColDesc("colbytesarray", "bytea[]"),
                new ColDesc("colbytesmaxarray", "bytea[]"),
                new ColDesc("coldatearray", "date[]"),
                new ColDesc("colfloat64array", "float8[]"),
                new ColDesc("colint64array", "bigint[]"),
                new ColDesc("colnumericarray", "numeric[]"),
                new ColDesc("colstringarray", "varchar(100)[]"),
                new ColDesc("colstringmaxarray", "varchar[]"),
                new ColDesc("coltimestamparray", "timestamptz[]"),
                new ColDesc("colfloat32", "real"),
                new ColDesc("coljson", "jsonb")
              }
              : new ColDesc[] {
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
                new ColDesc("ColFloat", "FLOAT32"),
                new ColDesc("ColInt", "INT64"),
                new ColDesc("ColMediumInt", "INT64"),
                new ColDesc("ColNumber", "NUMERIC"),
                new ColDesc("ColSmallInt", "INT64"),
                new ColDesc("ColTime", "TIMESTAMP"),
                new ColDesc("ColTinyInt", "INT64"),
                new ColDesc("ColUUID", "STRING(36)"),
                new ColDesc("ColXml", "STRING(MAX)"),
                new ColDesc("ColBoolArray", "ARRAY<BOOL>"),
                new ColDesc("ColBytesArray", "ARRAY<BYTES(100)>"),
                new ColDesc("ColBytesMaxArray", "ARRAY<BYTES(MAX)>"),
                new ColDesc("ColDateArray", "ARRAY<DATE>"),
                new ColDesc("ColFloat64Array", "ARRAY<FLOAT64>"),
                new ColDesc("ColInt64Array", "ARRAY<INT64>"),
                new ColDesc("ColNumericArray", "ARRAY<NUMERIC>"),
                new ColDesc("ColStringArray", "ARRAY<STRING(100)>"),
                new ColDesc("ColStringMaxArray", "ARRAY<STRING(MAX)>"),
                new ColDesc("ColTimestampArray", "ARRAY<TIMESTAMP>"),
                new ColDesc("ColFloat32", "FLOAT32"),
                new ColDesc("ColJson", "JSON")
              };
      testTableColumns(dialect, currentConnection, "TableWithAllLiquibaseTypes", cols);

      testTablePrimaryKeys(
          currentConnection, "TableWithAllLiquibaseTypes", new ColDesc("ColBigInt"));

      // Generate a snapshot of the database.
      SnapshotGeneratorFactory factory = SnapshotGeneratorFactory.getInstance();
      CatalogAndSchema schema =
          new CatalogAndSchema(
              dialect == Dialect.POSTGRESQL ? "test-database-id-pg" : "",
              dialect == Dialect.POSTGRESQL ? "public" : "");
      // CatalogAndSchema schema = new CatalogAndSchema("", "");
      SnapshotControl control = new SnapshotControl(liquibase.getDatabase());
      DatabaseSnapshot snapshot = factory.createSnapshot(schema, liquibase.getDatabase(), control);

      testSnapshotTableAndColumns(snapshot, "TableWithAllLiquibaseTypes", dialect, cols);
      testSnapshotPrimaryKey(
          snapshot,
          "TableWithAllLiquibaseTypes",
          new ColDesc(
              "ColBigInt", dialect == Dialect.POSTGRESQL ? "bigint" : "INT64", Boolean.FALSE));

      // Generate an initial changelog for the database.
      File changeLogFile = File.createTempFile("test-changelog", ".xml");
      changeLogFile.deleteOnExit();
      int returnCode =
          new LiquibaseCommandLine()
              .execute(
                  new String[] {
                    "--overwriteOutputFile=true",
                    String.format("--changeLogFile=%s", changeLogFile.getAbsolutePath()),
                    String.format("--url=%s", testHarness.getConnectionUrl()),
                    "generateChangeLog"
                  });
      assertThat(returnCode).isEqualTo(0);
      DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
      Document document = builder.parse(changeLogFile);
      testTableColumnsInXml(dialect, document, "TableWithAllLiquibaseTypes", cols);

      // Do rollback
      liquibase.rollback(1, null);

      // Ensure nothing is there!
      testTableColumns(dialect, currentConnection, "TableWithAllLiquibaseTypes");
    }
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorSpannerGenerateChangeLogForInterleavedTableTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doSpannerGenerateChangeLogForInterleavedTableTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerGenerateChangeLogForInterleavedTableTest() throws Exception {
    doSpannerGenerateChangeLogForInterleavedTableTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  private void doSpannerGenerateChangeLogForInterleavedTableTest(TestHarness.Connection testHarness)
      throws Exception {
    try (Connection connection = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = connection.createStatement()) {
        // Create a parent and child table manually.
        statement.addBatch(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers (SingerId bigint, Name varchar(200), PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers (SingerId INT64, Name STRING(200)) PRIMARY KEY (SingerId)");
        statement.addBatch(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Albums (SingerId bigint, AlbumId bigint, Title varchar, PRIMARY KEY (SingerId, AlbumId)) INTERLEAVE IN PARENT Singers"
                : "CREATE TABLE Albums (SingerId INT64, AlbumId INT64, Title STRING(MAX)) PRIMARY KEY (SingerId, AlbumId), INTERLEAVE IN PARENT Singers");
        statement.addBatch(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Concerts (ConcertId bigint, SingerId bigint, CONSTRAINT FK_Concerts_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId), PRIMARY KEY (ConcertId))"
                : "CREATE TABLE Concerts (ConcertId INT64, SingerId INT64, CONSTRAINT FK_Concerts_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId)) PRIMARY KEY (ConcertId)");
        statement.executeBatch();

        try {
          // Generate an initial changelog for the database.
          File changeLogFile = File.createTempFile("test-changelog", ".xml");
          changeLogFile.deleteOnExit();
          int returnCode =
              new LiquibaseCommandLine()
                  .execute(
                      new String[] {
                        "--overwriteOutputFile=true",
                        String.format("--changeLogFile=%s", changeLogFile.getAbsolutePath()),
                        String.format("--url=%s", testHarness.getConnectionUrl()),
                        "generateChangeLog"
                      });
          assertThat(returnCode).isEqualTo(0);

          // Verify that the generated change log only includes one foreign key.
          DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
          DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
          Document document = builder.parse(changeLogFile);
          XPathFactory xPathfactory = XPathFactory.newInstance();
          XPath xpath = xPathfactory.newXPath();
          XPathExpression expr =
              xpath.compile(String.format("//addForeignKeyConstraint", document));
          NodeList createForeignKeys = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
          assertEquals(1, createForeignKeys.getLength());
          Node createForeignKey = createForeignKeys.item(0);
          assertEquals(
              dialect == Dialect.POSTGRESQL ? "fk_concerts_singers" : "FK_Concerts_Singers",
              createForeignKey.getAttributes().getNamedItem("constraintName").getNodeValue());
        } finally {
          statement.addBatch("DROP TABLE Concerts");
          statement.addBatch("DROP TABLE Albums");
          statement.addBatch("DROP TABLE Singers");
          statement.executeBatch();
        }
      }
    }
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

  static void testTableColumns(
      Dialect dialect, java.sql.Connection conn, String table, ColDesc... cols)
      throws SQLException {

    boolean autocommitStatus = conn.getAutoCommit();
    boolean readOnlyStatus = conn.isReadOnly();
    conn.setAutoCommit(true);
    conn.setReadOnly(true);
    ResultSet rs = conn.getMetaData().getColumns(null, null, table, null);
    conn.setAutoCommit(autocommitStatus);
    conn.setReadOnly(readOnlyStatus);

    StringBuilder sql =
        new StringBuilder("SELECT COLUMN_NAME, SPANNER_TYPE AS TYPE_NAME, IS_NULLABLE \n")
            .append("FROM INFORMATION_SCHEMA.COLUMNS \n")
            .append("WHERE TABLE_CATALOG = ? \n")
            .append("AND TABLE_SCHEMA = ? \n")
            .append("AND TABLE_NAME = ? \n")
            .append("ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");

    PreparedStatement stmt = conn.prepareStatement(sql.toString());
    stmt.setString(1, "test-database-id-pg");
    stmt.setString(2, "public");
    stmt.setString(3, table.toLowerCase());
    ResultSet rs2 = stmt.executeQuery();
    List<Map<String, Object>> rows =
        dialect == Dialect.POSTGRESQL ? getResults(rs2) : getResults(rs);
    if (dialect == Dialect.POSTGRESQL) {
      for (Map<String, Object> row : rows) {
        String dataType = row.get("type_name").toString();
        dataType = dataType.replace("character varying", "varchar");
        dataType = dataType.replace("timestamp with time zone", "timestamptz");
        dataType = dataType.replace("double precision[]", "float8[]");
        dataType = dataType.replace("double precision", "float8");
        row.put("type_name", dataType);
      }
    }
    Assert.assertEquals(rows.size(), cols.length);
    for (int i = 0; i < cols.length; i++) {
      assertEquals(
          cols[i].name,
          dialect == Dialect.POSTGRESQL
              ? rows.get(i).get("column_name")
              : rows.get(i).get("COLUMN_NAME"));
      if (cols[i].type != null) {
        assertEquals(
            cols[i].type,
            dialect == Dialect.POSTGRESQL
                ? rows.get(i).get("type_name")
                : rows.get(i).get("TYPE_NAME"));
      }
      if (cols[i].isNullable != null) {
        String expectedValue = cols[i].isNullable ? "YES" : "NO";
        assertEquals(
            expectedValue,
            dialect == Dialect.POSTGRESQL
                ? rows.get(i).get("is_nullable")
                : rows.get(i).get("IS_NULLABLE"));
      }
    }
  }

  static void testTableColumnsInXml(
      Dialect dialect, Document document, String table, ColDesc... cols)
      throws XPathExpressionException {
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    XPathExpression expr =
        xpath.compile(
            String.format(
                "//createTable[@tableName=\"%s\"]",
                dialect == Dialect.POSTGRESQL ? table.toLowerCase() : table));
    NodeList createTables = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
    assertEquals(1, createTables.getLength());
    Node createTable = createTables.item(0);

    for (ColDesc col : cols) {
      testTableColumn(dialect, xpath, createTable, col);
    }
  }

  static void testTableColumn(Dialect dialect, XPath xpath, Node createTableNode, ColDesc col)
      throws XPathExpressionException {
    NodeList createCols =
        (NodeList)
            xpath
                .compile(String.format("//column[@name=\"%s\"]", col.name))
                .evaluate(createTableNode, XPathConstants.NODESET);
    assertEquals(1, createCols.getLength());
    Node createCol = createCols.item(0);
    assertEquals(col.name, createCol.getAttributes().getNamedItem("name").getNodeValue());
    assertEquals(
        col.type.toUpperCase(),
        createCol.getAttributes().getNamedItem("type").getNodeValue().toUpperCase());
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
          rows.get(i).get("COLUMN_NAME").toString().equalsIgnoreCase(cols[i].name), true);
    }
  }

  static void testSnapshotTableAndColumns(
      DatabaseSnapshot snapshot, String tableName, Dialect dialect, ColDesc... cols) {
    Table table =
        dialect == Dialect.POSTGRESQL
            ? snapshot.get(new Table("test-database-id-pg", "public", tableName))
            : snapshot.get(new Table("", "", tableName));
    assertThat(table).isNotNull();

    for (ColDesc col : cols) {
      Column column = snapshot.get(new Column(Table.class, "", "", tableName, col.name));
      assertThat(column).isNotNull();
      assertThat(column.getType().getTypeName()).isEqualTo(col.type);
      assertThat(column.isNullable()).isEqualTo(col.isNullable);
    }
  }

  static void testSnapshotPrimaryKey(DatabaseSnapshot snapshot, String tableName, ColDesc... cols) {
    Table table = snapshot.get(new Table("", "", tableName));
    assertThat(table.getPrimaryKey().getColumns()).hasSize(cols.length);
    List<Column> snapshotColumns = table.getPrimaryKey().getColumns();
    for (int i = 0; i < cols.length; i++) {
      Column column = snapshotColumns.get(i);
      assertThat(cols[i].name.toLowerCase()).isEqualTo(column.getName().toLowerCase());
      assertThat(cols[i].type).isEqualTo(column.getType().getTypeName());
      assertThat(cols[i].isNullable).isEqualTo(column.isNullable());
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
      Map<String, Object> rowVals = new HashMap<>();
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

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorSetNullableTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doSetNullableTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerSetNullableTest() throws Exception {
    doSetNullableTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doSetNullableTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection connection = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      // Create a simple table.
      Statement statement = connection.createStatement();
      statement.execute("START BATCH DDL");
      statement.execute(
          dialect == Dialect.POSTGRESQL
              ? "CREATE TABLE Singers (\n"
                  + "  SingerId bigint NOT NULL,\n"
                  + "  LastName varchar(100),\n"
                  + "PRIMARY KEY (SingerId))"
              : "CREATE TABLE Singers (\n"
                  + "  SingerId INT64 NOT NULL,\n"
                  + "  LastName STRING(100),\n"
                  + ") PRIMARY KEY (SingerId)");
      statement.execute("RUN BATCH");
      assertThat(isNullable(connection, "Singers", "LastName")).isTrue();

      // Run a Liquibase update to make the LastName column NOT NULL.
      try (Liquibase makeNotNull =
          getLiquibase(testHarness, "add-not-null-constraint-singers-lastname.spanner.yaml")) {
        makeNotNull.update(new Contexts("test"));
        Connection currentConnection =
            ((JdbcConnection) makeNotNull.getDatabase().getConnection()).getUnderlyingConnection();
        assertThat(isNullable(currentConnection, "Singers", "LastName")).isFalse();

        // Do rollback.
        makeNotNull.rollback(1, "test");
        assertThat(isNullable(currentConnection, "Singers", "LastName")).isTrue();

        // Manually make the column NOT NULL.
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "ALTER TABLE Singers ALTER COLUMN LastName TYPE varchar(100), ALTER COLUMN LastName SET NOT NULL"
                : "ALTER TABLE Singers ALTER COLUMN LastName STRING(100) NOT NULL");
        statement.execute("RUN BATCH");
        assertThat(isNullable(currentConnection, "Singers", "LastName")).isFalse();
      }

      try (Liquibase makeNullable =
          getLiquibase(testHarness, "drop-not-null-constraint-singers-lastname.spanner.yaml")) {
        makeNullable.update(new Contexts("test"));
        Connection currentConnection =
            ((JdbcConnection) makeNullable.getDatabase().getConnection()).getUnderlyingConnection();
        assertThat(isNullable(currentConnection, "Singers", "LastName")).isTrue();

        // Do rollback.
        makeNullable.rollback(1, "test");
        assertThat(isNullable(currentConnection, "Singers", "LastName")).isFalse();
      }

      statement.execute("START BATCH DDL");
      statement.execute("DROP TABLE Singers");
      statement.execute("RUN BATCH");
    }
  }

  static boolean isNullable(java.sql.Connection conn, String table, String column)
      throws SQLException {
    try (ResultSet rs = conn.getMetaData().getColumns(null, null, table, column)) {
      while (rs.next()) {
        return "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
      }
    }
    return false;
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void doEmulatorCreateViewTest(Dialect dialect) throws Exception {
    this.dialect = dialect;
    doCreateViewTest(getSpannerEmulator(dialect));
  }

  @Test
  @Tag("integration")
  void doRealSpannerCreateViewTest() throws Exception {
    doCreateViewTest(getSpannerReal(Dialect.GOOGLE_STANDARD_SQL));
  }

  void doCreateViewTest(TestHarness.Connection testHarness) throws Exception {
    try (Connection con = DriverManager.getConnection(testHarness.getConnectionUrl())) {
      try (Statement statement = con.createStatement()) {
        statement.execute("START BATCH DDL");
        statement.execute(
            dialect == Dialect.POSTGRESQL
                ? "CREATE TABLE Singers (SingerId bigint, FirstName varchar(100), LastName varchar(100), PRIMARY KEY (SingerId))"
                : "CREATE TABLE Singers (SingerId INT64, FirstName STRING(100), LastName STRING(100)) PRIMARY KEY (SingerId)");
        statement.execute("RUN BATCH");
        Object[][] singers =
            new Object[][] {
              {1L, "FirstName1", "c LastName1"},
              {2L, "FirstName2", "b LastName2"},
              {3L, "FirstName3", "a LastName3"},
            };
        statement.execute("BEGIN");
        try (PreparedStatement ps =
            con.prepareStatement(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (?, ?, ?)")) {
          for (Object[] singer : singers) {
            for (int p = 0; p < singer.length; p++) {
              // JDBC param index is 1-based.
              ps.setObject(p + 1, singer[p]);
            }
            ps.addBatch();
          }
          ps.executeBatch();
        }
        statement.execute("COMMIT");

        // The VIEW definition contains a LIMIT 2, so it will only return the first two rows.
        char[] prefixes = new char[] {'a', 'b'};
        try {
          Liquibase liquibase = getLiquibase(testHarness, "create-or-replace-view.spanner.yaml");
          liquibase.clearCheckSums();
          liquibase.update(new Contexts("test"));

          try (ResultSet rs = statement.executeQuery("SELECT * FROM V_Singers ORDER BY LastName")) {
            for (char prefix : prefixes) {
              assertThat(rs.next()).isTrue();
              assertThat(
                      rs.getString(dialect == Dialect.POSTGRESQL ? "lastname" : "LastName")
                          .toLowerCase())
                  .startsWith(String.format("%s LastName", prefix).toLowerCase());
            }
            assertThat(rs.next()).isFalse();
          }
        } finally {
          statement.execute("START BATCH DDL");
          statement.execute("DROP VIEW V_Singers");
          statement.execute("DROP TABLE Singers");
          statement.execute("RUN BATCH");
        }
      }
    }
  }
}
