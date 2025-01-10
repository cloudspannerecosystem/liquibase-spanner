/**
 * Copyright 2025 Google LLC
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
import liquibase.exception.ValidationErrors;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddDefaultValueGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.AddDefaultValueStatement;

public class AddDefaultValueGeneratorSpanner extends AddDefaultValueGenerator {
  static final String ADD_DEFAULT_VALUE_VALIDATION_ERROR =
      "Cloud Spanner supports default values for columns, but only with built-in functions such as CURRENT_TIMESTAMP() and GENERATE_UUID()";

  @Override
  public ValidationErrors validate(
      AddDefaultValueStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(statement, database, sqlGeneratorChain);
    if (statement.getDefaultValue() instanceof DatabaseFunction) {
      return errors;
    }
    errors.addError(ADD_DEFAULT_VALUE_VALIDATION_ERROR);
    return errors;
  }

  @Override
  public Sql[] generateSql(final AddDefaultValueStatement statement, final Database database, SqlGeneratorChain sqlGeneratorChain) {
    Object defaultValue = statement.getDefaultValue();
    StringBuilder queryStringBuilder = new StringBuilder();
    queryStringBuilder.append("ALTER TABLE ");
    queryStringBuilder.append(database.escapeSequenceName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()));
    queryStringBuilder.append(" ALTER COLUMN ");
    queryStringBuilder.append(statement.getColumnName());
    queryStringBuilder.append(" SET DEFAULT (");
    queryStringBuilder.append(defaultValue);
    queryStringBuilder.append(")");

    return new Sql[]{new UnparsedSql(queryStringBuilder.toString(), getAffectedColumn(statement))};
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(AddDefaultValueStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }
}
