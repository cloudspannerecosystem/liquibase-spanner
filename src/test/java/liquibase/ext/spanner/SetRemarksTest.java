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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.spanner.Dialect;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.exception.CommandExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class SetRemarksTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testSetTableRemarksFromYaml(Dialect dialect) throws Exception {
    for (String file : new String[] {"set-table-remarks.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        CommandExecutionException exception =
            assertThrows(
                CommandExecutionException.class,
                () -> liquibase.update(new Contexts("test"), new OutputStreamWriter(System.out)));
        assertThat(exception.getMessage()).contains("setTableRemarks is not supported");
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testSetColumnRemarksFromYaml(Dialect dialect) throws Exception {
    for (String file : new String[] {"set-column-remarks.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        CommandExecutionException exception =
            assertThrows(
                CommandExecutionException.class,
                () -> liquibase.update(new Contexts("test"), new OutputStreamWriter(System.out)));
        assertThat(exception.getMessage()).contains("setColumnRemarks is not supported");
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }
}
