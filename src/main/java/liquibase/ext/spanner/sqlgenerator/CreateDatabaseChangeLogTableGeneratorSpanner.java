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
import java.sql.SQLException;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;

public class CreateDatabaseChangeLogTableGeneratorSpanner
    extends CreateDatabaseChangeLogTableGenerator {

  final String createTableSql =
      ""
          + "CREATE TABLE __DATABASECHANGELOG__\n"
          + "(\n"
          + "    id            string(MAX) not null,\n"
          + "    author        string(MAX) not null,\n"
          + "    filename      string(MAX) not null,\n"
          + "    dateExecuted  timestamp   not null,\n"
          + "    orderExecuted int64       not null,\n"
          + "    execType      string(MAX),\n"
          + "    md5sum        string(MAX),\n"
          + "    description   string(MAX),\n"
          + "    comments      string(MAX),\n"
          + "    tag           string(MAX),\n"
          + "    liquibase     string(MAX),\n"
          + "    contexts      string(MAX),\n"
          + "    labels        string(MAX),\n"
          + "    deployment_id string(MAX),\n"
          + ") primary key (id, author, filename);";

  final String createPostgresqlTableSql =
      ""
          + "CREATE TABLE public.__DATABASECHANGELOG__\n"
          + "(\n"
          + "    id            varchar(255) not null,\n"
          + "    author        varchar(255) not null,\n"
          + "    filename      varchar(255) not null,\n"
          + "    dateExecuted  timestamptz  not null,\n"
          + "    orderExecuted bigint       not null,\n"
          + "    execType      varchar(255),\n"
          + "    md5sum        varchar(255),\n"
          + "    description   varchar(255),\n"
          + "    comments      varchar(255),\n"
          + "    tag           varchar(255),\n"
          + "    liquibase     varchar(255),\n"
          + "    contexts      varchar(255),\n"
          + "    labels        varchar(255),\n"
          + "    deployment_id varchar(255),\n"
          + "    PRIMARY KEY (id, author, filename)\n"
          + ");";

  @Override
  public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public ValidationErrors validate(
      CreateDatabaseChangeLogTableStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    return new ValidationErrors();
  }

  @Override
  public Sql[] generateSql(
      CreateDatabaseChangeLogTableStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    String databaseChangeLogTableName = getDatabaseChangeLogTableNameFrom(database);

    Dialect dialect = null;
    try {
      dialect = ((ICloudSpanner) database).getDialect();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    String createTableSQL = "";
    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      createTableSQL =
          this.createTableSql.replaceAll("__DATABASECHANGELOG__", databaseChangeLogTableName);
    } else if (dialect == Dialect.POSTGRESQL) {
      createTableSQL =
          this.createPostgresqlTableSql.replaceAll(
              "__DATABASECHANGELOG__", databaseChangeLogTableName);
    }

    return new Sql[] {new UnparsedSql(createTableSQL)};
  }

  private String getDatabaseChangeLogTableNameFrom(Database database) {
    String databaseChangeLogTableName = database.getDatabaseChangeLogTableName();
    if (databaseChangeLogTableName == null) {
      databaseChangeLogTableName = "DATABASECHANGELOG";
    }
    return databaseChangeLogTableName;
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
