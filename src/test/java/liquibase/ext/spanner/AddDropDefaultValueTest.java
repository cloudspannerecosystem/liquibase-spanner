/**
 * Copyright 2025 Google LLC
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import java.text.ParseException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.util.ISODateFormat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class AddDropDefaultValueTest extends AbstractMockServerTest {

  @BeforeAll
  static void setupResults() {
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_COLUMN_DEFAULT_VALUE)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("Singers") // Table name
                .bind("p4")
                .to("uuid_column") // Column Name
                .build(),
            JdbcMetadataQueries.createGetColumnDefaultValueResultSet(
                ImmutableList.of(
                    new JdbcMetadataQueries.ColumnDefaultValueMetadata("GENERATE_UUID()")))));
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_COLUMN_DEFAULT_VALUE)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("%") // Table name
                .bind("p4")
                .to("%") // Column Name
                .build(),
            JdbcMetadataQueries.createGetColumnDefaultValueResultSet(
                ImmutableList.of(new JdbcMetadataQueries.ColumnDefaultValueMetadata(null)))));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testAddDefaultValueFromYaml() throws Exception {
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers ALTER COLUMN LastName SET DEFAULT ('some-name')",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"add-default-value-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(expectedSql.length);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }

  @Test
  void testAddDefaultValueBooleanFromYaml() throws Exception {
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers ADD booleanColumn BOOL",
          "ALTER TABLE Singers ALTER COLUMN booleanColumn SET DEFAULT (TRUE)",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"add-default-value-boolean-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(expectedSql.length);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }

  @Test
  void testAddDefaultValueNumericFromYaml() throws Exception {
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers ADD numericColumn INT64",
          "ALTER TABLE Singers ALTER COLUMN numericColumn SET DEFAULT (1000000)",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"add-default-value-numeric-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(expectedSql.length);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }

  @Test
  void testAddDefaultValueComputedFromYaml() throws Exception {
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers ADD uuid_column STRING(36)",
          "ALTER TABLE Singers ALTER COLUMN uuid_column SET DEFAULT (GENERATE_UUID())",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"add-default-value-computed-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(expectedSql.length);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }

  @Test
  void testAddDefaultValueTimestampFromYaml() throws Exception {
    String timestamp = convertToUtcTimestamp("2008-02-12T12:34:03");
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers ADD timestampColumn timestamp",
          "ALTER TABLE Singers ALTER COLUMN timestampColumn SET DEFAULT (TIMESTAMP "
              + timestamp
              + ")",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"add-default-value-date-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(expectedSql.length);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }

  @Test
  void testCreateColumnWithDefaultValueFromYaml() throws Exception {
    String expectedSql =
        "ALTER TABLE Singers ADD stringColumn STRING(1000) DEFAULT ('some_string')";

    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-column-with-default-value.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(request.getStatementsList()).hasSize(1);
    assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql);
  }

  @Test
  void testCreateColumnWithDefaultValueBooleanFromYaml() throws Exception {
    String expectedSql = "ALTER TABLE Singers ADD booleanColumn BOOL DEFAULT (TRUE)";

    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-column-with-default-value-boolean.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(request.getStatementsList()).hasSize(1);
    assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql);
  }

  @Test
  void testCreateColumnWithDefaultValueNumericFromYaml() throws Exception {
    String expectedSql = "ALTER TABLE Singers ADD numericColumn INT64 DEFAULT (100000)";

    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-column-with-default-value-numeric.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(request.getStatementsList()).hasSize(1);
    assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql);
  }

  @Test
  void testCreateColumnWithDefaultValueComputedFromYaml() throws Exception {
    String expectedSql = "ALTER TABLE Singers ADD uuid_column STRING(36) DEFAULT (GENERATE_UUID())";

    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-column-with-default-value-computed.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(request.getStatementsList()).hasSize(1);
    assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql);
  }

  @Test
  void testCreateColumnWithDefaultValueDateFromYaml() throws Exception {
    String timestamp = convertToUtcTimestamp("2008-02-12T12:34:03");
    String expectedSql =
        "ALTER TABLE Singers ADD timestampColumn timestamp DEFAULT (TIMESTAMP " + timestamp + ")";

    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-column-with-default-value-date.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(request.getStatementsList()).hasSize(1);
    assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql);
  }

  @Test
  void testDropDefaultValueFromYaml() throws Exception {
    String expectedSql = "ALTER TABLE Singers ALTER COLUMN LastName DROP DEFAULT";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"drop-default-value-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(request.getStatementsList()).hasSize(1);
    assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql);
  }

  private String convertToUtcTimestamp(String dateTime) throws ParseException {
    Date date = new ISODateFormat().parse(dateTime);
    Instant instant = date.toInstant();
    OffsetDateTime utcDateTime = instant.atOffset(ZoneOffset.UTC);
    String formattedDate = utcDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE);
    String formattedTime = utcDateTime.format(DateTimeFormatter.ISO_LOCAL_TIME);

    // Return the TIMESTAMP string in SQL format
    return "'" + formattedDate + "T" + formattedTime + "Z'";
  }
}
