package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.io.IOException;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateTableInOtherSchemaTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @BeforeAll
  static void startStaticServer() throws IOException {
    startStaticServer("liquibaseschema", "otherschema");
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateSingersTableFromYaml(Dialect dialect) throws Exception {
    String expectedSql =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE otherschema.Singers (SingerId bigint, FirstName varchar(255), LastName varchar(255) NOT NULL, SingerInfo bytea, \"hash`s\" varchar(40), PRIMARY KEY (SingerId))"
            : "CREATE TABLE otherschema.Singers (SingerId INT64, FirstName STRING(255), LastName STRING(255) NOT NULL, SingerInfo BYTES(MAX), `hash\\`s` STRING(40)) PRIMARY KEY (SingerId)";
    addUpdateDdlStatementsResponse(dialect, expectedSql);

    for (String file : new String[] {"create-singers-table.spanner.yaml"}) {
      try (Connection con = createConnection(dialect);
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.getDatabase().setDefaultSchemaName("otherschema");
        liquibase.getDatabase().setLiquibaseSchemaName("liquibaseschema");

        // Update to version v0.1.
        liquibase.update(new Contexts("test"), new LabelExpression("version 0.1"));

        // Register result for tagging the last update and then tag it.
        mockSpanner.putStatementResult(
            StatementResult.update(
                Statement.of(
                    "UPDATE liquibaseschema.DATABASECHANGELOG SET TAG = 'rollback-v0.1' WHERE DATEEXECUTED = (SELECT MAX(DATEEXECUTED) FROM liquibaseschema.DATABASECHANGELOG)"),
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
}
