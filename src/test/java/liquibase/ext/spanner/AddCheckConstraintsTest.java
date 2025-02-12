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
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.sql.Connection;

import static com.google.common.truth.Truth.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
public class AddCheckConstraintsTest extends AbstractMockServerTest{

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testCreateTableWithCheckConstraintsFromYaml() throws Exception {
    String expectedSql =
        "CREATE TABLE Concerts (\n" +
            "ConcertId INT64,\n" +
            "StartTime Timestamp,\n" +
            "EndTime Timestamp,\n" +
            "CONSTRAINT start_before_end CHECK(StartTime < EndTime),\n" +
            ") PRIMARY KEY (ConcertId)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-table-with-check-constraint.spanner.yaml"}) {
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
  void testAddCheckConstraintsFromYaml() throws Exception {
    String expectedSql =
        "ALTER TABLE Singers\n" +
            "ADD CONSTRAINT concert_id_gt_0 CHECK (ConcertId > 0)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"add-check-constraint-singers.spanner.yaml"}) {
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
