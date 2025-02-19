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
package liquibase.ext.spanner.sqlgenerator;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import liquibase.database.Database;
import liquibase.sql.Sql;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CreateDatabaseChangeLogTableGeneratorSpannerTest {

  private static final String DEFAULT_CHANGELOG = "DATABASECHANGELOG";
  private static final String CUSTOM_CHANGELOG = "customChangeLog";

  private Database database = Mockito.mock(Database.class);

  private final CreateDatabaseChangeLogTableGeneratorSpanner generator =
      new CreateDatabaseChangeLogTableGeneratorSpanner();

  @Test
  public void shouldUseConfiguredDatabaseChangeLogTableName() {
    givenDatabaseChangeLogTableNameIs(CUSTOM_CHANGELOG);

    Sql[] sql = generator.generateSql(null, database, null);

    assertTrue(sql[0].toSql().startsWith("CREATE TABLE " + CUSTOM_CHANGELOG));
  }

  @Test
  public void shouldUseDefaultDatabaseChangeLogTableNameIfNotConfigured() {
    givenDatabaseChangeLogTableNameIs(null);

    Sql[] sql = generator.generateSql(null, database, null);

    assertTrue(sql[0].toSql().startsWith("CREATE TABLE " + DEFAULT_CHANGELOG));
  }

  private void givenDatabaseChangeLogTableNameIs(String tableName) {
    when(database.getDatabaseChangeLogTableName()).thenReturn(tableName);
  }
}
