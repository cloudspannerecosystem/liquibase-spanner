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

import com.google.common.base.MoreObjects;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;

public class ModifyDataTypeGeneratorSpanner extends ModifyDataTypeGenerator {

  @Override
  public boolean supports(ModifyDataTypeStatement statement, Database database) {
    return database instanceof CloudSpanner;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(ModifyDataTypeStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    // Cloud Spanner requires NOT NULL to be specified if the column is to retain any NOT NULL constraint.
    String nullableString = isColumnNullable(statement, database) ? "" : " NOT NULL";
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
        + DataTypeFactory.getInstance().fromDescription(statement.getNewDataType(), database).toDatabaseDataType(database)
        + nullableString;
    
    return new Sql[] {new UnparsedSql(sql, getAffectedTable(statement))};
  }
  
  private boolean isColumnNullable(ModifyDataTypeStatement statement, Database database) {
    JdbcConnection connection = (JdbcConnection) database.getConnection();
    try (PreparedStatement ps = connection.prepareStatement("SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG=? AND TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?")) {
      ps.setString(1, MoreObjects.firstNonNull(statement.getCatalogName(), ""));
      ps.setString(2, MoreObjects.firstNonNull(statement.getSchemaName(), ""));
      ps.setString(3, statement.getTableName());
      ps.setString(4, statement.getColumnName());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return "YES".equalsIgnoreCase(rs.getString(1));
        } else {
          throw new UnexpectedLiquibaseException(String.format("Column not found: %s.%s", statement.getTableName(), statement.getColumnName()));
        }
      }
    } catch (SQLException | DatabaseException e) {
      throw new UnexpectedLiquibaseException(String.format("Could not retrieve column information for column %s.%s", statement.getTableName(), statement.getColumnName()), e);
    }
    
  }
}