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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TempMockSpannerServiceImpl.StatementResult;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class SetNullableConstraintTest extends AbstractMockServerTest {

  @BeforeAll
  static void setupColumnQueryResults() {
    // Add a result for the lookup of the LastName column.
    mockSpanner.putPartialStatementResult(
        StatementResult.query(
            Statement.of(
                "SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,"),
            createLastNameColumnResultSet()));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  private static ResultSet createLastNameColumnResultSet() {
    return ResultSet.newBuilder()
        .setMetadata(
            ResultSetMetadata.newBuilder()
                .setRowType(
                    StructType.newBuilder()
                        .addFields(
                            Field.newBuilder()
                                .setName("TYPE_NAME")
                                .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                                .build())
                        .build())
                .build())
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue("STRING(100)").build())
                .build())
        .build();
  }

  @Test
  void testAddSingersLastNameNotNullFromYaml() throws Exception {
    String expectedSql = "ALTER TABLE Singers ALTER COLUMN LastName STRING(100) NOT NULL";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"add-not-null-constraint-singers-lastname.spanner.yaml"}) {
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
  void testDropSingersLastNameNotNullFromYaml() throws Exception {
    // Dropping a NOT NULL constraint in Cloud Spanner is done by not adding the NOT NULL constraint
    // at the end of the column definition.
    String expectedSql = "ALTER TABLE Singers ALTER COLUMN LastName STRING(100)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"drop-not-null-constraint-singers-lastname.spanner.yaml"}) {
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
