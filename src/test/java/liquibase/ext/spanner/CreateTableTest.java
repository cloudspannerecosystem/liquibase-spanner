package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TempMockSpannerServiceImpl.StatementResult;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.exception.ValidationFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateTableTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testCreateSingersTableFromYaml() throws Exception {
    String expectedSql =
        "CREATE TABLE Singers (SingerId INT64 NOT NULL, FirstName STRING(255), LastName STRING(255) NOT NULL, SingerInfo BYTES(MAX)) PRIMARY KEY (SingerId)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-singers-table.spanner.yaml"}) {
      try (Connection con = createConnection();
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

  @Test
  void testCreateTableWithAllColumnTypesFromYaml() throws Exception {
    String expectedSql =
        "CREATE TABLE TableWithAllColumnTypes (ColInt64 INT64 NOT NULL, ColFloat64 FLOAT64, ColBool BOOL, ColString STRING(200), ColStringMax STRING(MAX), ColBytes BYTES(200), ColBytesMax BYTES(MAX), ColDate date, ColTimestamp timestamp, ColNumeric NUMERIC) PRIMARY KEY (ColInt64)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-table-with-all-spanner-types.spanner.yaml"}) {
      try (Connection con = createConnection();
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

  @Test
  void testCreateTableWithAllLiquibaseTypesFromYaml() throws Exception {
    String expectedSql =
        "CREATE TABLE TableWithAllLiquibaseTypes (ColBigInt INT64 NOT NULL, ColBlob BYTES(MAX), ColBoolean BOOL, ColChar STRING(100), ColNChar STRING(50), ColNVarchar STRING(100), ColVarchar STRING(200), ColClob STRING(MAX), ColDateTime TIMESTAMP, ColTimestamp timestamp, ColDate date, ColDecimal DECIMAL, ColDouble FLOAT64, ColFloat FLOAT64, ColInt INT64, ColMediumInt INT64, ColNumber NUMERIC, ColSmallInt INT64, ColTime TIMESTAMP, ColTinyInt INT64, ColUUID STRING(36), ColXml STRING(MAX)) PRIMARY KEY (ColBigInt)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"create-table-with-all-liquibase-types.spanner.yaml"}) {
      try (Connection con = createConnection();
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

  @Test
  void testTableWithoutPrimaryKeyFromYaml() throws Exception {
    for (String file : new String[] {"create-table-without-pk.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"), new LabelExpression("version 0.1"));
        fail("missing expected validation exception");
      } catch (ValidationFailedException e) {
        assertThat(e.getMessage()).contains("primary key is required");
      }
    }
  }
}
