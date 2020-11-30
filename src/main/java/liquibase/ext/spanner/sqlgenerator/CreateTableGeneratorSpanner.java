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
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.DatabaseObject;
import org.apache.commons.lang3.StringUtils;

public class CreateTableGeneratorSpanner extends CreateTableGenerator {

  public CreateTableGeneratorSpanner() {}

  @Override
  public ValidationErrors validate(
      CreateTableStatement createTableStatement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(createTableStatement, database, sqlGeneratorChain);
    // Cloud Spanner requires a primary key to be defined. Cloud Spanner allows the list of columns
    // of the primary key constraint to be empty, but that cannot be defined in the metamodel of
    // Liquibase.
    errors.checkRequiredField("primary key", createTableStatement.getPrimaryKeyConstraint());

    return errors;
  }

  @Override
  public Sql[] generateSql(
      CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    Sql[] res = super.generateSql(statement, database, sqlGeneratorChain);
    // Move the PRIMARY KEY statement from inside the table creation to outside.
    StringBuilder buffer = new StringBuilder(", PRIMARY KEY (");
    buffer.append(
        database.escapeColumnNameList(
            StringUtils.join(statement.getPrimaryKeyConstraint().getColumns(), ", ")));
    buffer.append(")");

    String pk = buffer.toString();
    String sql = res[0].toSql();
    sql = sql.replace(pk, "");
    // Append PRIMARY KEY (without the leading ,)
    sql = sql + pk.substring(1);

    return new Sql[] {
      new UnparsedSql(
          sql,
          res[0]
              .getAffectedDatabaseObjects()
              .toArray(new DatabaseObject[res[0].getAffectedDatabaseObjects().size()]))
    };
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
