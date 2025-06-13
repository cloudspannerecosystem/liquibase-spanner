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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class AddCheckConstraintsTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateTableWithCheckConstraintsFromYaml(Dialect dialect) throws Exception {

    // mockSpanner.putStatementResult(MockSpannerServiceImpl.StatementResult.update(com.google.cloud.spanner.Statement.of("update foo set bar=1 where true"), 10000L));

    String[] expectedSql =
        dialect == Dialect.POSTGRESQL
            ? new String[] {
              "CREATE TABLE Concerts (ConcertId bigint NOT NULL, StartTime timestamptz, EndTime timestamptz, PRIMARY KEY (ConcertId))",
              "ALTER TABLE Concerts ADD CONSTRAINT start_before_end CHECK (StartTime < EndTime)"
            }
            : new String[] {
              "CREATE TABLE Concerts (ConcertId INT64 NOT NULL, StartTime TIMESTAMP, EndTime TIMESTAMP) PRIMARY KEY (ConcertId)",
              "ALTER TABLE Concerts ADD CONSTRAINT start_before_end CHECK (StartTime < EndTime)"
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(dialect, sql);
    }

    mockSpanner.putStatementResult(
        MockSpannerServiceImpl.StatementResult.detectDialectResult(dialect));
    for (String file : new String[] {"create-table-with-check-constraint.spanner.yaml"}) {

      try (Connection con = createConnection(dialect);
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

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testAddCheckConstraintsFromYaml(Dialect dialect) throws Exception {
    String expectedSql =
        "ALTER TABLE Singers\n" + "ADD CONSTRAINT concert_id_gt_0 CHECK (ConcertId > 0)";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"add-check-constraint-singers.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
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
