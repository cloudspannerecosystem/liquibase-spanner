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

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import java.sql.Connection;

import static com.google.common.truth.Truth.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateTableWithGeneratedColumns extends AbstractMockServerTest {
  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testCreateTableWithGeneratedColumnFromYaml() throws Exception {
    String[] expectedSql =
        new String[] {
            "CREATE TABLE table_test_generated_column (id INT64 NOT NULL, FirstName STRING(200), LastName STRING(200), FullName STRING(400) AS (FirstName || ' ' || LastName) STORED) PRIMARY KEY (id)"
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"create-table-with-generated-column.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
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
