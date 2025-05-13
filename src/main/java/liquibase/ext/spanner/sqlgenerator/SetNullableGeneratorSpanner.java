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
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.SetNullableGenerator;
import liquibase.statement.core.SetNullableStatement;

public class SetNullableGeneratorSpanner extends SetNullableGenerator {

  @Override
  public boolean supports(SetNullableStatement statement, Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(
      SetNullableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    // Cloud Spanner requires the full column definition to change nullability,
    // as it does not support standalone ADD/DROP NULLABLE clauses.
    // For nullable columns in Cloud Spanner, nullability is implicit (i.e., not explicitly stated).
    // For PostgreSQL dialect, use ALTER COLUMN SET/DROP NOT NULL instead.
    Dialect dialect = ((ICloudSpanner) database).getDialect();
    boolean isNullable = statement.isNullable();

    // Determine the correct SQL clause for altering nullability based on dialect
    String nullableClause = "";
    if (dialect == Dialect.POSTGRESQL) {
      nullableClause = isNullable ? " DROP NOT NULL" : " SET NOT NULL";
    } else if (!isNullable) {
      nullableClause = " NOT NULL";
    }

    // Build data type string
    String dataType =
        DataTypeFactory.getInstance()
            .fromDescription(statement.getColumnDataType(), database)
            .toDatabaseDataType(database)
            .toString();

    if (dialect == Dialect.POSTGRESQL) {
      dataType = "TYPE " + dataType;
    }

    // Escape names
    String catalog = statement.getCatalogName();
    String schema = statement.getSchemaName();
    String tableName = statement.getTableName();
    String columnName = statement.getColumnName();

    String escapedTableName = database.escapeTableName(catalog, schema, tableName);
    String escapedColumnName = database.escapeColumnName(catalog, schema, tableName, columnName);

    // Construct the final ALTER TABLE ... ALTER COLUMN SQL statement
    StringBuilder sql = new StringBuilder();
    sql.append("ALTER TABLE ")
        .append(escapedTableName)
        .append(" ALTER COLUMN ")
        .append(escapedColumnName)
        .append(" ")
        .append(dataType);

    if (dialect == Dialect.POSTGRESQL) {
      sql.append(", ALTER COLUMN ").append(escapedColumnName).append(nullableClause);
    } else {
      sql.append(nullableClause);
    }
    return new Sql[] {new UnparsedSql(String.valueOf(sql), getAffectedColumn(statement))};
  }
}
