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

import liquibase.database.Database;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateViewGenerator;
import liquibase.statement.core.CreateViewStatement;

public class CreateViewGeneratorSpanner extends CreateViewGenerator {

  @Override
  public Sql[] generateSql(
      CreateViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    StringBuilder sql = new StringBuilder();
    if (!statement.isFullDefinition()) {
      sql.append("CREATE ");
      if (statement.isReplaceIfExists()) {
        sql.append("OR REPLACE ");
      }
      sql.append("VIEW ")
          .append(
              database.escapeViewName(
                  statement.getCatalogName(), statement.getSchemaName(), statement.getViewName()))
          .append(" SQL SECURITY INVOKER AS ");
    }
    sql.append(statement.getSelectQuery());

    return new Sql[] {new UnparsedSql(sql.toString(), getAffectedView(statement))};
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(CreateViewStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }
}
