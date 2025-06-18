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
import com.google.cloud.spanner.connection.AbstractStatementParser;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class DropAllForeignKeysTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testDropAllForeignKeysFromSingers(Dialect dialect) throws Exception {
    registerStatement(dialect);
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers DROP CONSTRAINT FK_Singers1",
          "ALTER TABLE Singers DROP CONSTRAINT FK_Singers2",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(dialect, sql);
    }

    for (String file : new String[] {"drop-all-foreign-key-constraints-singers.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
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
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testDropForeignKeyTest(Dialect dialect) throws Exception {
    String[] expectedSql =
        new String[] {
          "ALTER TABLE Singers DROP CONSTRAINT FK_Singers1",
        };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(dialect, sql);
    }

    for (String file : new String[] {"drop-foreign-key.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
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
  }

  private static void registerStatement(Dialect dialect) {
    String catalog = dialect == Dialect.POSTGRESQL ? "db_pg" : "";
    String schema = dialect == Dialect.POSTGRESQL ? "public" : "";
    final String sql =
        "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_CATALOG=? AND TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_TYPE='FOREIGN KEY'";
    AbstractStatementParser parser = dialect == Dialect.POSTGRESQL ? PARSER_PG : PARSER;

    AbstractStatementParser.ParametersInfo params =
        parser.convertPositionalParametersToNamedParameters('?', sql);
    final ResultSetMetadata FIND_FOREIGN_KEYS_METADATA =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("TABLE_CAT")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                    .build())
            .build();
    final ResultSet FIND_FOREIGN_KEYS_RESULT =
        ResultSet.newBuilder()
            .setMetadata(FIND_FOREIGN_KEYS_METADATA)
            .addRows(
                ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("FK_Singers1")))
            .addRows(
                ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("FK_Singers2")))
            .build();

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("Singers")
                .build(),
            FIND_FOREIGN_KEYS_RESULT));
  }
}
