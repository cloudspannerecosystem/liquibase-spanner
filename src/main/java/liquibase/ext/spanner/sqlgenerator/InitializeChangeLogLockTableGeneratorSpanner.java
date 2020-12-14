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

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DeleteStatement;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.InsertStatement;

public class InitializeChangeLogLockTableGeneratorSpanner
    extends AbstractSqlGenerator<InitializeDatabaseChangeLogLockTableStatement> {

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public ValidationErrors validate(
      InitializeDatabaseChangeLogLockTableStatement statement,
      Database database,
      SqlGeneratorChain<InitializeDatabaseChangeLogLockTableStatement> sqlGenerator) {
    return new ValidationErrors();
  }

  @Override
  public Sql[] generateSql(
      InitializeDatabaseChangeLogLockTableStatement statement,
      Database database,
      SqlGeneratorChain<InitializeDatabaseChangeLogLockTableStatement> sqlGenerator) {
    return SqlGeneratorFactory.getInstance()
        .generateSql(
            new SqlStatement[] {
              new DeleteStatement(
                      database.getLiquibaseCatalogName(),
                      database.getLiquibaseSchemaName(),
                      database.getDatabaseChangeLogLockTableName())
                  .setWhere("true"),
              new InsertStatement(
                      database.getLiquibaseCatalogName(),
                      database.getLiquibaseSchemaName(),
                      database.getDatabaseChangeLogLockTableName())
                  .addColumnValue("ID", 1)
                  .addColumnValue("LOCKED", Boolean.FALSE)
            },
            database);
  }
}
