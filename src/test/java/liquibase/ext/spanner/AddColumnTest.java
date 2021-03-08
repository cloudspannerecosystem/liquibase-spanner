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

import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class AddColumnTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testAddSingerInfoToSingersFromYaml() throws Exception {
    // The following statement does not include the COLUMN keyword. According to the Cloud Spanner
    // documentation the keyword is required, but the documentation is slightly off here. The COLUMN
    // keyword is actually optional in Cloud Spanner (as in most other DBMS's).
    String expectedSql = "ALTER TABLE Singers ADD COLUMN SingerInfo BYTES(MAX)";
    addUpdateDdlStatementsResponse(expectedSql);

    for (String file : new String[] {"add-singerinfo-to-singers-table.spanner.yaml"}) {
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
  void testAddTrackAndLyricsToSongsFromYaml() throws Exception {
    String[] expectedSql =
            new String[]{
                    "ALTER TABLE Songs ADD COLUMN Track INT64 NOT NULL", "ALTER TABLE Songs ADD COLUMN Lyrics STRING(MAX)"
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[]{"add-track-and-lyrics-to-songs-table.spanner.yaml"}) {
      try (Connection con = createConnection();
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

  @Test
  void testAddSingerToConcertsFromYaml() throws Exception {
    String[] expectedSql =
            new String[]{
                    "ALTER TABLE Concerts ADD COLUMN SingerId INT64 NOT NULL",
                    "ALTER TABLE Concerts ADD CONSTRAINT FK_Concerts_Singer FOREIGN KEY (SingerId) REFERENCES Singers (SingerId)"
            };
    for (String sql : expectedSql) {
      addUpdateDdlStatementsResponse(sql);
    }

    for (String file : new String[]{"add-singer-to-concerts-table.spanner.yaml"}) {
      try (Connection con = createConnection();
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
}
