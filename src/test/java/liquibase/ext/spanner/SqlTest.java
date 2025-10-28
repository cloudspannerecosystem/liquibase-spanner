/**
 * Copyright 2024 Google LLC
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
import com.google.spanner.v1.ExecuteSqlRequest;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class SqlTest extends AbstractMockServerTest {
  private static final String INSERT1 =
      "insert into my_table (id, name, other_languages)\n"
          + "select 1, 'One', ['En', 'Eins', 'Uno']\n"
          + "union all\n"
          + "select 2, 'Two', ['To', 'Zwei', 'Dos']";
  private static final String INSERT2 =
      "insert into my_table (id, name, other_languages)\n"
          + "select 3, 'Three', ['Tre', 'Drei', 'Tres']\n"
          + "union all\n"
          + "select 4, 'Four', ['Fire', 'Vier', 'Cuatro']";

  @BeforeAll
  static void setupResults() {
    mockSpanner.putStatementResult(
        StatementResult.update(
            Statement.of("UPDATE DATABASECHANGELOG SET MD5SUM = NULL WHERE true"), 0L));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(INSERT1), 1L));
    mockSpanner.putStatementResult(StatementResult.update(Statement.of(INSERT2), 1L));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void estExecuteSql(Dialect dialect) throws Exception {
    for (String file : new String[] {"sql.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.clearCheckSums();
        liquibase.update(new Contexts("test"));
      }
    }
    List<ExecuteSqlRequest> requests =
        mockSpanner.getRequestsOfType(ExecuteSqlRequest.class).stream()
            .filter(request -> request.getSql().equals(INSERT1) || request.getSql().equals(INSERT2))
            .collect(Collectors.toList());
    assertThat(requests.size()).isEqualTo(2);
  }
}
