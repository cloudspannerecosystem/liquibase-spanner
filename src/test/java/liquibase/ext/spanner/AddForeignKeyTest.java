package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class AddForeignKeyTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testAddFKAlbumsSingersFromYaml() throws Exception {
    // There's an extra space in the expected SQL as Liquibase will always try to add a constraint
    // name. Cloud Spanner does not support adding a constraint name to a foreign key, so an empty
    // string is rendered, but with a trailing space.
    String expectedSql =
        "ALTER TABLE Albums ADD CONSTRAINT  FOREIGN KEY (SingerId) REFERENCES Singers (SingerId)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"add-foreign-key-albums-singers.spanner.yaml"}) {
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
  void testAddFKSongsAlbumsFromYaml() throws Exception {
    String expectedSql =
        "ALTER TABLE Songs ADD CONSTRAINT  FOREIGN KEY (SingerId, AlbumId) REFERENCES Albums (SingerId, AlbumId)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"add-foreign-key-songs-albums.spanner.yaml"}) {
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
