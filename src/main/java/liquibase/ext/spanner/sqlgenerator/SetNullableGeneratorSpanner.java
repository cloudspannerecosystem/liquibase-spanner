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
package liquibase.ext.spanner;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetNullableGenerator;
import liquibase.statement.core.SetNullableStatement;

public class SetNullableGeneratorSpanner extends SetNullableGenerator {

  @Override
  public boolean supports(SetNullableStatement statement, Database database) {
    return database instanceof CloudSpanner;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(SetNullableStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    // Cloud Spanner does not have any specific syntax for adding/dropping NULLABLE constraints.
    // Instead, an ALTER COLUMN statement must be generated that includes the entire column
    // definition. For nullable columns, nothing should be specified regarding nullability.
    String nullableString = statement.isNullable() ? " " : " NOT NULL";
    String sql = "ALTER TABLE "
        + database.escapeTableName(
            statement.getCatalogName(),
            statement.getSchemaName(),
            statement.getTableName())
        + " ALTER COLUMN "
        + database.escapeColumnName(
            statement.getCatalogName(),
            statement.getSchemaName(),
            statement.getTableName(),
            statement.getColumnName())
        + " "
        + DataTypeFactory.getInstance().fromDescription(statement.getColumnDataType(), database).toDatabaseDataType(database)
        + nullableString;

    return new Sql[] {new UnparsedSql(sql, getAffectedColumn(statement))};
  }
}
