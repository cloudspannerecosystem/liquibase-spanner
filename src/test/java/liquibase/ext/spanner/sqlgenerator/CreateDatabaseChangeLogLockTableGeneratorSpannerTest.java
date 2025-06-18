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

import com.google.cloud.spanner.Dialect;
import java.sql.SQLException;
import liquibase.ext.spanner.CloudSpanner;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

class CreateDatabaseChangeLogLockTableGeneratorSpannerTest {

  private static final String DEFAULT_CHANGELOGLOCK = "DATABASECHANGELOGLOCK";
  private static final String CUSTOM_CHANGELOGLOCK = "customChangeLogLock";

  private ICloudSpanner database = Mockito.mock(CloudSpanner.class);

  private final CreateDatabaseChangeLogLockTableGeneratorSpanner generator =
      new CreateDatabaseChangeLogLockTableGeneratorSpanner();

  @ParameterizedTest
  @EnumSource(Dialect.class)
  public void shouldUseConfiguredDatabaseChangeLogLockTableName(Dialect dialect)
      throws SQLException {
    givenDatabaseChangeLogLockTableNameIs(CUSTOM_CHANGELOGLOCK);
    when(database.getDialect()).thenReturn(dialect);

    Sql[] sql = generator.generateSql(null, database, null);

    assertTrue(
        sql[0]
            .toSql()
            .startsWith(
                dialect == Dialect.POSTGRESQL
                    ? "CREATE TABLE public." + CUSTOM_CHANGELOGLOCK
                    : "CREATE TABLE " + CUSTOM_CHANGELOGLOCK));
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  public void shouldUseDefaultDatabaseChangeLogTableNameIfNotConfigured(Dialect dialect)
      throws SQLException {
    givenDatabaseChangeLogLockTableNameIs(null);
    when(database.getDialect()).thenReturn(dialect);

    Sql[] sql = generator.generateSql(null, database, null);

    assertTrue(
        sql[0]
            .toSql()
            .startsWith(
                dialect == Dialect.POSTGRESQL
                    ? "CREATE TABLE public." + DEFAULT_CHANGELOGLOCK
                    : "CREATE TABLE " + DEFAULT_CHANGELOGLOCK));
  }

  private void givenDatabaseChangeLogLockTableNameIs(String tableName) {
    when(database.getDatabaseChangeLogLockTableName()).thenReturn(tableName);
  }
}
