/**
 * Copyright 2020 Google LLC
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package liquibase.ext.spanner.sqlgenerator;

import java.util.Date;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.InsertStatement;

/** Generator for INSERT statements in the form 'INSERT INTO FOO (..) SELECT ...'. */
public class InsertWithSelectGeneratorSpanner extends InsertGenerator {

  @Override
  public int getPriority() {
    // This generator should only be used manually.
    return 0;
  }

  @Override
  public Sql[] generateSql(InsertStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    // Generate INSERT INTO (...) header.
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ").append(database.escapeTableName(statement.getCatalogName(),
        statement.getSchemaName(), statement.getTableName())).append(" (");
    boolean first = true;
    for (String column : statement.getColumnValues().keySet()) {
      if (first) {
        first = false;
      } else {
        sql.append(", ");
      }
      sql.append(database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
          statement.getTableName(), column));
    }
    sql.append(")");

    // Generate SELECT ... statement.
    sql.append(" SELECT ");
    first = true;
    for (String column : statement.getColumnValues().keySet()) {
      if (first) {
        first = false;
      } else {
        sql.append(", ");
      }
      Object newValue = statement.getColumnValues().get(column);
      if ((newValue == null) || "NULL".equalsIgnoreCase(newValue.toString())) {
        sql.append("NULL");
      } else if ((newValue instanceof String)
          && !looksLikeFunctionCall(((String) newValue), database)) {
        sql.append(DataTypeFactory.getInstance().fromObject(newValue, database)
            .objectToSql(newValue, database));
      } else if (newValue instanceof Date) {
        sql.append(database.getDateLiteral(((Date) newValue)));
      } else if (newValue instanceof Boolean) {
        if (((Boolean) newValue)) {
          sql.append(DataTypeFactory.getInstance().getTrueBooleanValue(database));
        } else {
          sql.append(DataTypeFactory.getInstance().getFalseBooleanValue(database));
        }
      } else if (newValue instanceof DatabaseFunction) {
        sql.append(database.generateDatabaseFunctionValue((DatabaseFunction) newValue));
      } else {
        sql.append(newValue);
      }
    }

    return new Sql[] {new UnparsedSql(sql.toString(), getAffectedTable(statement))};
  }

}
