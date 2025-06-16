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
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateTableWithGeneratedColumns extends AbstractMockServerTest {
  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateTableWithGeneratedColumnFromYaml(Dialect dialect) throws Exception {
    String[] expectedSql =
        dialect == Dialect.POSTGRESQL
            ? new String[] {
              "CREATE TABLE table_test_generated_column (id bigint NOT NULL, FirstName varchar(200), LastName varchar(200), FullName varchar(400) generated always as (FirstName || ' ' || LastName) STORED, PRIMARY KEY (id))"
            }
            : new String[] {
              "CREATE TABLE table_test_generated_column (id INT64 NOT NULL, FirstName STRING(200), LastName STRING(200), FullName STRING(400) AS (FirstName || ' ' || LastName) STORED) PRIMARY KEY (id)"
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(dialect, sql);
    }

    for (String file :
        new String[] {
          dialect == Dialect.POSTGRESQL
              ? "create-table-with-generated-column-pg.spanner.yaml"
              : "create-table-with-generated-column.spanner.yaml"
        }) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        // Update to version v0.1.
        liquibase.update(new Contexts("test"), new LabelExpression("version 0.1"));
      }
    }
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }
}
