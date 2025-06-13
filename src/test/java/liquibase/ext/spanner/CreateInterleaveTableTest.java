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
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateInterleaveTableTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateInterleaveTablesFromYaml(Dialect dialect) throws Exception {
    String[] expectedSql =
        dialect == Dialect.POSTGRESQL
            ? new String[] {
              "CREATE TABLE Singers (\n"
                  + "SingerId   bigint NOT NULL PRIMARY KEY,\n"
                  + "FirstName  varchar(1024),\n"
                  + "LastName   varchar(1024),\n"
                  + "SingerInfo bytea,\n"
                  + ")",
              "CREATE TABLE Albums (\n"
                  + "SingerId     bigint NOT NULL,\n"
                  + "AlbumId      bigint NOT NULL,\n"
                  + "AlbumTitle   varchar,\n"
                  + "PRIMARY KEY (SingerId, AlbumId))\n"
                  + "INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
            }
            : new String[] {
              "CREATE TABLE Singers (\n"
                  + "SingerId   INT64 NOT NULL PRIMARY KEY,\n"
                  + "FirstName  STRING(1024),\n"
                  + "LastName   STRING(1024),\n"
                  + "SingerInfo BYTES(MAX),\n"
                  + ")",
              "CREATE TABLE Albums (\n"
                  + "SingerId     INT64 NOT NULL,\n"
                  + "AlbumId      INT64 NOT NULL,\n"
                  + "AlbumTitle   STRING(MAX),\n"
                  + ") PRIMARY KEY (SingerId, AlbumId),\n"
                  + "INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(dialect, sql);
    }

    for (String file :
        new String[] {
          dialect == Dialect.POSTGRESQL
              ? "create-interleave-table-pg.spanner.yaml"
              : "create-interleave-table.spanner.yaml"
        }) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    assertThat(mockAdmin.getRequests()).hasSize(1);
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    assertThat(request.getStatementsList()).hasSize(2);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(request.getStatementsList().get(i)).isEqualTo(expectedSql[i]);
    }
  }
}
