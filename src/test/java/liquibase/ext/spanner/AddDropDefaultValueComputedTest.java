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
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import java.sql.Connection;

@Execution(ExecutionMode.SAME_THREAD)
public class AddDropDefaultValueComputedTest extends  AbstractMockServerTest {

  @BeforeAll
  static void setupResults(){
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_COLUMN_DEFAULT_VALUE)
                .bind("p1").to("") // Catalog
                .bind("p2").to("") // Schema
                .bind("p3").to("Singers") // Table name
                .bind("p4").to("uuid_column") // Column Name
                .build(),
            JdbcMetadataQueries.createGetColumnDefaultValueResultSet(
                ImmutableList.of(
                    new JdbcMetadataQueries.ColumnDefaultValueMetadata("GENERATE_UUID()")
                )
            )
        ));
    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_COLUMN_DEFAULT_VALUE)
                .bind("p1").to("") // Catalog
                .bind("p2").to("") // Schema
                .bind("p3").to("%") // Table name
                .bind("p4").to("%") // Column Name
                .build(),
            JdbcMetadataQueries.createGetColumnDefaultValueResultSet(
                ImmutableList.of(
                    new JdbcMetadataQueries.ColumnDefaultValueMetadata(null)
                )
            )
        ));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testAddDefaultValueComputedFromYaml() throws Exception {
    String[] expectedSql =
        new String[] {
            "ALTER TABLE Singers ADD COLUMN uuid_column STRING(36)",
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
  void testCreateColumnWithDefaultValueComputedFromYaml() throws Exception {
    String expectedSql = "ALTER TABLE Singers ADD COLUMN uuid_column STRING(36) DEFAULT (GENERATE_UUID())";

    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-column-with-default-value-computed.yaml"}) {
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
  void testDropDefaultValueComputedFromYaml() throws Exception {
    String expectedSql =
        "ALTER TABLE Singers ALTER COLUMN uuid_colum DROP DEFAULT";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"drop-default-value-computed-singers.spanner.yaml"}) {
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
}
