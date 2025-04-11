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

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;

public class CreateDatabaseChangeLogLockTableGeneratorSpanner
    extends CreateDatabaseChangeLogLockTableGenerator {

  final String createTableSQL =
      ""
          + "CREATE TABLE __DATABASECHANGELOGLOCK__\n"
          + "(\n"
          + "    id          int64,\n"
          + "    locked      bool,\n"
          + "    lockgranted timestamp,\n"
          + "    lockedby    string(max),\n"
          + ") primary key (id)";

  final String createPostgresqlTableSQL =
      ""
          + "CREATE TABLE public.__DATABASECHANGELOGLOCK__\n"
          + "(\n"
          + "    id          bigint primary key,\n"
          + "    locked      bool,\n"
          + "    lockgranted timestamptz,\n"
          + "    lockedby    varchar\n"
          + ")";

  @Override
  public Sql[] generateSql(
      CreateDatabaseChangeLogLockTableStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {

    Dialect dialect = ((ICloudSpanner) database).getDialect();

    String databaseChangeLogLockTableName = getDatabaseChangeLogLockTableNameFrom(database);
    String createTableSQL =
        dialect == Dialect.POSTGRESQL ? this.createPostgresqlTableSQL : this.createTableSQL;
    createTableSQL =
        createTableSQL.replaceAll("__DATABASECHANGELOGLOCK__", databaseChangeLogLockTableName);

    return new Sql[] {new UnparsedSql(createTableSQL)};
  }

  private String getDatabaseChangeLogLockTableNameFrom(Database database) {
    String databaseChangeLogLockTableName = database.getDatabaseChangeLogLockTableName();
    if (databaseChangeLogLockTableName == null) {
      databaseChangeLogLockTableName = "DATABASECHANGELOGLOCK";
    }
    return databaseChangeLogLockTableName;
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }
}
