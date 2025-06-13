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
public class ModifyDataTypeTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testModifySingersSingerInfoToStringFromYaml(Dialect dialect) throws Exception {
    registerStatement(dialect);
    String expectedSql =
        dialect == Dialect.POSTGRESQL
            ? "ALTER TABLE Singers ALTER COLUMN SingerInfo TYPE varchar, ALTER COLUMN SingerInfo DROP NOT NULL"
            : "ALTER TABLE Singers ALTER COLUMN SingerInfo STRING(MAX)";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"modify-data-type-singers-singerinfo.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
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

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testModifySingersLastNameToLongerStringFromYaml(Dialect dialect) throws Exception {
    String expectedSql =
        dialect == Dialect.POSTGRESQL
            ? "ALTER TABLE Singers ALTER COLUMN LastName TYPE varchar(1000), ALTER COLUMN LastName SET NOT NULL"
            : "ALTER TABLE Singers ALTER COLUMN LastName STRING(1000) NOT NULL";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"modify-data-type-singers-lastname.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
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

  private static void registerStatement(Dialect dialect) {
    String catalog = dialect == Dialect.POSTGRESQL ? "db_pg" : "";
    String schema = dialect == Dialect.POSTGRESQL ? "public" : "";
    AbstractStatementParser parser = dialect == Dialect.POSTGRESQL ? PARSER_PG : PARSER;
    final String FIND_COLUMN =
        "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG=? AND TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?";
    AbstractStatementParser.ParametersInfo findColumnParams =
        parser.convertPositionalParametersToNamedParameters('?', FIND_COLUMN);

    final ResultSetMetadata FIND_COLUMN_METADATA =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("IS_NULLABLE")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                    .build())
            .build();
    final ResultSet NULLABLE_RESULT =
        ResultSet.newBuilder()
            .setMetadata(FIND_COLUMN_METADATA)
            .addRows(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("YES")))
            .build();
    final ResultSet NOT_NULLABLE_RESULT =
        ResultSet.newBuilder()
            .setMetadata(FIND_COLUMN_METADATA)
            .addRows(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("NO")))
            .build();

    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(findColumnParams.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("Singers")
                .bind("p4")
                .to("SingerInfo")
                .build(),
            NULLABLE_RESULT));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(findColumnParams.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("Singers")
                .bind("p4")
                .to("LastName")
                .build(),
            NOT_NULLABLE_RESULT));
  }
}
