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
import liquibase.exception.ValidationErrors;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;

public class CreateDatabaseChangeLogTableGeneratorSpanner
    extends CreateDatabaseChangeLogTableGenerator {

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
    Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);
    if (sqls == null || sqls.length == 0) {
      // This should never happen, but just to be safe.
      return sqls;
    }

    Dialect dialect = ((ICloudSpanner) database).getDialect();
    String sql = sqls[0].toSql();
    if (dialect == Dialect.POSTGRESQL) {
      int index = sql.lastIndexOf(')');
      if (index != -1) {
        sql = sql.substring(0, index);
        sql += ",\nprimary key (id, author, filename))";
      }
    } else {
      sql += " primary key (ID, AUTHOR, FILENAME)";
    }
    return new Sql[] {new UnparsedSql(sql)};
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
