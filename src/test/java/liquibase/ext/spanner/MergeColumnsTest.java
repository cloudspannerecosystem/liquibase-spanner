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
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.AbstractMessage;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.BeginTransactionRequest;
import java.sql.Connection;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import liquibase.Contexts;
import liquibase.Liquibase;

@Execution(ExecutionMode.SAME_THREAD)
public class MergeColumnsTest extends AbstractMockServerTest {

  @BeforeAll
  static void setupResults() {
    mockSpanner.putStatementResult(StatementResult.update(
        Statement
            .of("UPDATE Singers SET FullName = FirstName || 'some-string' || LastName WHERE TRUE"),
        10));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testMergeSingersFirstNamdAndLastNameFromYaml() throws Exception {
    String[] expectedSql = new String[] {
        "ALTER TABLE Singers ADD FullName STRING(500)",
        "ALTER TABLE Singers DROP COLUMN FirstName",
        "ALTER TABLE Singers DROP COLUMN LastName",
      };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[] {"merge-singers-firstname-and-lastname.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
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
    // Verify that the mock server received one BeginTransactionRequest for a PDML transaction.
    assertThat(mockSpanner.getRequests().stream().filter(new Predicate<AbstractMessage>() {
      @Override
      public boolean test(AbstractMessage t) {
        return t instanceof BeginTransactionRequest;
      }
    }).map(new Function<AbstractMessage, BeginTransactionRequest>() {
      @Override
      public BeginTransactionRequest apply(AbstractMessage t) {
        return (BeginTransactionRequest) t;
      }
    }).filter(new Predicate<BeginTransactionRequest>() {
      @Override
      public boolean test(BeginTransactionRequest t) {
        return t.hasOptions() && t.getOptions().hasPartitionedDml();
      }
    }).count()).isEqualTo(1L);
  }
}
