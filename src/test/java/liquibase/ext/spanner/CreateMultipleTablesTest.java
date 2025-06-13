package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import java.util.Arrays;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class CreateMultipleTablesTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testCreateMultipleTablesFromYaml(Dialect dialect) throws Exception {
    String createSingers =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE Singers (SingerId bigint, FirstName varchar(255), LastName varchar(255) NOT NULL, SingerInfo bytea, PRIMARY KEY (SingerId))"
            : "CREATE TABLE Singers (SingerId INT64, FirstName STRING(255), LastName STRING(255) NOT NULL, SingerInfo BYTES(MAX)) PRIMARY KEY (SingerId)";
    String createAlbums =
        dialect == Dialect.POSTGRESQL
            ? "CREATE TABLE Albums (AlbumId bigint, Title varchar(255), Singer bigint, PRIMARY KEY (AlbumId))"
            : "CREATE TABLE Albums (AlbumId INT64, Title STRING(255), Singer INT64) PRIMARY KEY (AlbumId)";
    addUpdateDdlStatementsResponse(dialect, Arrays.asList(createSingers, createAlbums));

    for (String file : new String[] {"create-multiple-tables.spanner.yaml"}) {
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
    assertThat(request.getStatementsList()).hasSize(2);
    assertThat(request.getStatementsList().get(0)).isEqualTo(createSingers);
    assertThat(request.getStatementsList().get(1)).isEqualTo(createAlbums);
  }
}
