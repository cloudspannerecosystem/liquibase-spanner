package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import java.sql.SQLException;
import liquibase.Liquibase;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class SimpleMockServerTest extends AbstractMockServerTest {

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateTableStatement(Dialect dialect) throws SQLException {
    String statement =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE FOO (ID bigint, PRIMARY KEY (ID))"
            : "CREATE TABLE FOO (ID INT64) PRIMARY KEY (ID)";
    addUpdateDdlStatementsResponse(dialect, statement);

    try (Connection con = createConnection(dialect); ) {
      con.createStatement()
          .execute(
              dialect == Dialect.POSTGRESQL
                  ? "CREATE TABLE FOO (ID bigint, PRIMARY KEY (ID))"
                  : "CREATE TABLE FOO (ID INT64) PRIMARY KEY (ID)");
    }
    assertThat(mockAdmin.getRequests()).hasSize(dialect == Dialect.POSTGRESQL ? 2 : 1);
    assertThat(mockAdmin.getRequests().get(dialect == Dialect.POSTGRESQL ? 1 : 0))
        .isInstanceOf(UpdateDatabaseDdlRequest.class);
    assertThat(getUpdateDdlStatementsList(dialect == Dialect.POSTGRESQL ? 1 : 0))
        .containsExactly(statement)
        .inOrder();

    // This test does not use Liquibase but JDBC directly, so it is expected to send requests that
    // do not contain a Liquibase client lib token.
    assertThat(receivedRequestWithNonLiquibaseToken.get()).isTrue();
    // Clear the flag to prevent the check after each test to fail.
    receivedRequestWithNonLiquibaseToken.set(false);
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testInitLiquibase(Dialect dialect) throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_COUNT_FROM_DATABASECHANGELOG, createInt64ResultSet(0L)));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_FROM_DATABASECHANGELOG,
            DatabaseChangeLog.createChangeSetResultSet(ImmutableList.of())));
    try (Connection con = createConnection(dialect);
        Liquibase liquibase = getLiquibase(con, "changelog.spanner.sql")) {
      liquibase.validate();
    }
  }
}
