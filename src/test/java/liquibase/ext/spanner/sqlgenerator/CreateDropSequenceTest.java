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
package liquibase.ext.spanner.sqlgenerator;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.exception.CommandExecutionException;
import liquibase.ext.spanner.AbstractMockServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateDropSequenceTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testCreateSequenceFromYaml() throws Exception {
    ImmutableList<String> expectedSql = ImmutableList.of("CREATE SEQUENCE IdSequence " 
        + "OPTIONS (sequence_kind='bit_reversed_positive', "
        + "start_with_counter = 100000)",
        "CREATE SEQUENCE MinimalSequence OPTIONS (sequence_kind='bit_reversed_positive')");
    addUpdateDdlStatementsResponse(expectedSql.get(0));
    addUpdateDdlStatementsResponse(expectedSql.get(1));
    
    for (String file : new String[]{"create-sequence.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }
    
    assertEquals(2, mockAdmin.getRequests().size());
    
    assertEquals(UpdateDatabaseDdlRequest.class, mockAdmin.getRequests().get(0).getClass());
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertEquals(1, request.getStatementsCount());
    assertEquals(expectedSql.get(0), request.getStatementsList().get(0));
    
    assertEquals(UpdateDatabaseDdlRequest.class, mockAdmin.getRequests().get(0).getClass());
    request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(1);
    assertEquals(1, request.getStatementsCount());
    assertEquals(expectedSql.get(1), request.getStatementsList().get(0));
  }

  @Test
  void testDropSequenceFromYaml() throws Exception {
    String expectedSql = "DROP SEQUENCE IdSequence";
    addUpdateDdlStatementsResponse(expectedSql);
    
    for (String file : new String[]{"drop-sequence.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }
    
    assertEquals(1, mockAdmin.getRequests().size());
    UpdateDatabaseDdlRequest request = (UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(0);
    assertEquals(1, request.getStatementsCount());
    assertEquals(expectedSql, request.getStatementsList().get(0));
  }

  @Test
  void testRenameSequenceFromYaml() throws Exception {
    for (String file : new String[]{"rename-sequence.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
        CommandExecutionException exception = assertThrows(CommandExecutionException.class,
            () -> liquibase.update(new Contexts("test"), new OutputStreamWriter(System.out)));
        assertThat(exception.getMessage()).contains(
            RenameSequenceGeneratorSpanner.RENAME_SEQUENCE_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }
}
