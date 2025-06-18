/**
 * Copyright 2020 Google LLC
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
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Predicate;
import com.google.protobuf.AbstractMessage;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class AddLookupTableTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testAddLookupTableSingersCountriesFromYaml(Dialect dialect) throws Exception {
    String[] expectedSql =
        dialect == Dialect.POSTGRESQL
            ? new String[] {
              "CREATE TABLE Countries (Name varchar(100) NOT NULL, PRIMARY KEY (Name))",
              "ALTER TABLE Singers ADD CONSTRAINT FK_Singers_Countries FOREIGN KEY (Country) REFERENCES Countries (Name)",
            }
            : new String[] {
              "CREATE TABLE Countries (Name STRING(100) NOT NULL) PRIMARY KEY (Name)",
              "ALTER TABLE Singers ADD CONSTRAINT FK_Singers_Countries FOREIGN KEY (Country) REFERENCES Countries (Name)",
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(dialect, sql);
    }
    final String insert =
        "INSERT INTO Countries (Name) SELECT DISTINCT Country FROM Singers WHERE Country IS NOT NULL";
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(insert), 100L));

    for (String file : new String[] {"add-lookup-table-singers-countries.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    mockSpanner.waitForRequestsToContain(
        new Predicate<AbstractMessage>() {
          @Override
          public boolean apply(AbstractMessage input) {
            if (input instanceof ExecuteSqlRequest) {
              ExecuteSqlRequest request = (ExecuteSqlRequest) input;
              return request.getSql().equals(insert);
            }
            return false;
          }
        },
        500L);
    assertThat(mockAdmin.getRequests()).hasSize(expectedSql.length);
    for (int i = 0; i < expectedSql.length; i++) {
      assertThat(mockAdmin.getRequests().get(i)).isInstanceOf(UpdateDatabaseDdlRequest.class);
      UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(i);
      assertThat(request.getStatementsList()).hasSize(1);
      assertThat(request.getStatementsList().get(0)).isEqualTo(expectedSql[i]);
    }
  }
}
