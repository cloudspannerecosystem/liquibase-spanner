package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.TempMockSpannerServiceImpl.StatementResult;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import java.sql.SQLException;
import liquibase.Liquibase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class SimpleMockServerTest extends AbstractMockServerTest {

  @Test
  void testCreateTableStatement() throws SQLException {
    String statement = "CREATE TABLE FOO (ID INT64) PRIMARY KEY (ID)";
    addUpdateDdlStatementsResponse(statement);

    try (Connection con = createConnection()) {
      con.createStatement().execute("CREATE TABLE FOO (ID INT64) PRIMARY KEY (ID)");
    }
    assertThat(mockAdmin.getRequests()).hasSize(1);
    assertThat(mockAdmin.getRequests().get(0)).isInstanceOf(UpdateDatabaseDdlRequest.class);
    assertThat(getUpdateDdlStatementsList(0)).containsExactly(statement).inOrder();
  }

  @Test
  void testInitLiquibase() throws Exception {
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_COUNT_FROM_DATABASECHANGELOG, createInt64ResultSet(0L)));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_FROM_DATABASECHANGELOG,
            DatabaseChangeLog.createChangeSetResultSet(ImmutableList.of())));
    try (Connection con = createConnection();
        Liquibase liquibase = getLiquibase(con, "changelog.spanner.sql")) {
      liquibase.validate();
    }
  }
}
