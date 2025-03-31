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

import java.sql.SQLException;
import liquibase.ext.spanner.CloudSpanner;
import liquibase.sql.Sql;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CreateDatabaseChangeLogLockTableGeneratorSpannerTest {

  private static final String DEFAULT_CHANGELOGLOCK = "DATABASECHANGELOGLOCK";
  private static final String CUSTOM_CHANGELOGLOCK = "customChangeLogLock";

  private CloudSpanner database = Mockito.mock(CloudSpanner.class);

  private final CreateDatabaseChangeLogLockTableGeneratorSpanner generator =
      new CreateDatabaseChangeLogLockTableGeneratorSpanner();

  // TODO: add tests for PostgreSQL dialect
  @Test
  public void shouldUseConfiguredDatabaseChangeLogLockTableName() throws SQLException {
    givenDatabaseChangeLogLockTableNameIs(CUSTOM_CHANGELOGLOCK);
    when(database.getDialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);

    Sql[] sql = generator.generateSql(null, database, null);

    assertTrue(sql[0].toSql().startsWith("CREATE TABLE " + CUSTOM_CHANGELOGLOCK));
  }

  @Test
  public void shouldUseDefaultDatabaseChangeLogTableNameIfNotConfigured() throws SQLException {
    givenDatabaseChangeLogLockTableNameIs(null);
    when(database.getDialect()).thenReturn(com.google.cloud.spanner.Dialect.GOOGLE_STANDARD_SQL);

    Sql[] sql = generator.generateSql(null, database, null);

    assertTrue(sql[0].toSql().startsWith("CREATE TABLE " + DEFAULT_CHANGELOGLOCK));
  }

  private void givenDatabaseChangeLogLockTableNameIs(String tableName) {
    when(database.getDatabaseChangeLogLockTableName()).thenReturn(tableName);
  }
}
