package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.exception.CommandExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateTableTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateSingersTableFromYaml(Dialect dialect) throws Exception {
    String expectedSql =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE Singers (SingerId bigint, FirstName varchar(255), LastName varchar(255) NOT NULL, SingerInfo bytea, `hash\\`s` varchar(40), PRIMARY KEY (SingerId))"
            : "CREATE TABLE Singers (SingerId INT64, FirstName STRING(255), LastName STRING(255) NOT NULL, SingerInfo BYTES(MAX), `hash\\`s` STRING(40)) PRIMARY KEY (SingerId)";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"create-singers-table.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        // Update to version v0.1.
        liquibase.update(new Contexts("test"), new LabelExpression("version 0.1"));

        // Register result for tagging the last update and then tag it.
        mockSpanner.putStatementResult(
            StatementResult.update(
                Statement.of(
                    "UPDATE DATABASECHANGELOG SET TAG = 'rollback-v0.1' WHERE DATEEXECUTED = (SELECT MAX(DATEEXECUTED) FROM DATABASECHANGELOG)"),
                1L));
        liquibase.tag("rollback-v0.1");
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
  void testCreateTableWithAllColumnTypesFromYaml(Dialect dialect) throws Exception {
    String expectedSql =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE TableWithAllColumnTypes (ColInt64 bigint NOT NULL, ColFloat64 float8, ColBool boolean, ColString varchar(200), ColStringMax varchar, ColBytes bytea, ColBytesMax bytea, ColDate date, ColTimestamp timestamptz, ColNumeric NUMERIC, PRIMARY KEY (ColInt64))"
            : "CREATE TABLE TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColBool BOOL, ColString STRING(200), ColStringMax STRING(MAX), ColBytes BYTES(200), ColBytesMax BYTES(MAX), ColDate date, ColTimestamp TIMESTAMP, ColNumeric NUMERIC) PRIMARY KEY (ColInt64)";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"create-table-with-all-spanner-types.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"), new LabelExpression("version 0.2"));
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
  void testCreateTableWithAllLiquibaseTypesFromYaml(Dialect dialect) throws Exception {
    String expectedSql =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE TableWithAllLiquibaseTypes (ColBigInt bigint NOT NULL, ColBlob bytea, ColBoolean boolean, ColChar varchar(100), ColNChar varchar(50), ColNVarchar varchar(100), ColVarchar varchar(200), ColClob varchar, ColDateTime timestamptz, ColTimestamp timestamptz, ColDate date, ColDecimal NUMERIC, ColDouble float8, ColFloat real, ColInt bigint, ColMediumInt int, ColNumber NUMERIC, ColSmallInt bigint, ColTime timestamptz, ColTinyInt bigint, ColUUID varchar(36), ColXml varchar, ColBoolArray boolean[], ColBytesArray bytea[], ColBytesMaxArray bytea[], ColDateArray date[], ColFloat64Array float8[], ColInt64Array bigint[], ColNumericArray numeric[], ColStringArray varchar(100)[], ColStringMaxArray varchar[], ColTimestampArray timestamptz[], ColFloat32 real, ColJson jsonb, PRIMARY KEY (ColBigInt))"
            : "CREATE TABLE TableWithAllLiquibaseTypes (ColBigInt INT64 NOT NULL, ColBlob BYTES(MAX), ColBoolean BOOL, ColChar STRING(100), ColNChar STRING(50), ColNVarchar STRING(100), ColVarchar STRING(200), ColClob STRING(MAX), ColDateTime TIMESTAMP, ColTimestamp TIMESTAMP, ColDate date, ColDecimal NUMERIC, ColDouble FLOAT64, ColFloat FLOAT32, ColInt INT64, ColMediumInt INT64, ColNumber NUMERIC, ColSmallInt INT64, ColTime TIMESTAMP, ColTinyInt INT64, ColUUID STRING(36), ColXml STRING(MAX), ColBoolArray ARRAY<BOOL>, ColBytesArray ARRAY<BYTES(100)>, ColBytesMaxArray ARRAY<BYTES(MAX)>, ColDateArray ARRAY<DATE>, ColFloat64Array ARRAY<FLOAT64>, ColInt64Array ARRAY<INT64>, ColNumericArray ARRAY<NUMERIC>, ColStringArray ARRAY<STRING(100)>, ColStringMaxArray ARRAY<STRING(MAX)>, ColTimestampArray ARRAY<TIMESTAMP>, ColFloat32 FLOAT32, ColJson JSON) PRIMARY KEY (ColBigInt)";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"create-table-with-all-liquibase-types.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"), new LabelExpression("version 0.3"));
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
  void testTableWithoutPrimaryKeyFromYaml(Dialect dialect) throws Exception {
    for (String file : new String[] {"create-table-without-pk.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        CommandExecutionException exception =
            assertThrows(
                CommandExecutionException.class,
                () ->
                    liquibase.update(
                        new Contexts("test"),
                        new LabelExpression("version 0.1"),
                        new OutputStreamWriter(System.out)));
        assertThat(exception.getMessage()).contains("primary key is required");
      }
    }
  }
}
