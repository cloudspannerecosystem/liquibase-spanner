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
package liquibase.ext.spanner.snapshotgenerator;

import com.google.cloud.spanner.Dialect;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;

public class ColumnSnapshotGeneratorSpanner extends ColumnSnapshotGenerator {
  @Override
  public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    if (database instanceof ICloudSpanner) {
      return PRIORITY_DATABASE;
    }
    return PRIORITY_NONE;
  }

  @Override
  protected Object readDefaultValue(
      CachedRow columnMetadataResultSet, Column columnInfo, Database database) {
    if (database instanceof ICloudSpanner) {
      try {
        // TODO: Remove when COLUMN_DEF is included in the results for getColumns
        String selectQuery =
            "SELECT DISTINCT COLUMN_DEFAULT AS COLUMN_DEF FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_CATALOG = ? "
                + "AND TABLE_SCHEMA = ? "
                + "AND TABLE_NAME = ? "
                + "AND COLUMN_NAME = ?";
        String schemaName =
            columnInfo.getRelation().getSchema().getName() == null
                ? database.getDefaultSchemaName()
                : columnInfo.getRelation().getSchema().getName();
        String defaultValue =
            Scope.getCurrentScope()
                .getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database)
                .queryForObject(
                    new RawParameterizedSqlStatement(
                        selectQuery,
                        new Object[] {
                          columnInfo.getRelation().getSchema().getCatalog().getName(),
                          schemaName,
                          columnInfo.getRelation().getName(),
                          columnInfo.getName()
                        }),
                    String.class);
        if (defaultValue != null) {
          if (database.isFunction(defaultValue)) {
            columnMetadataResultSet.set("COLUMN_DEF", new DatabaseFunction((defaultValue)));
          } else {
            columnMetadataResultSet.set("COLUMN_DEF", defaultValue);
          }
        }
      } catch (DatabaseException e) {
        Scope.getCurrentScope()
            .getLog(this.getClass())
            .warning("Error fetching default column values", e);
      }
    }
    return super.readDefaultValue(columnMetadataResultSet, columnInfo, database);
  }

  @Override
  protected DataType readDataType(
      CachedRow columnMetadataResultSet, Column column, Database database)
      throws DatabaseException {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      if (dialect == Dialect.POSTGRESQL) {
        try {
          StringBuilder sql =
              new StringBuilder("SELECT SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS\n")
                  .append("WHERE TABLE_SCHEMA = ?\n")
                  .append("AND TABLE_NAME = ?\n")
                  .append("AND COLUMN_NAME = ?");
          String dataType =
              Scope.getCurrentScope()
                  .getSingleton(ExecutorService.class)
                  .getExecutor("jdbc", database)
                  .queryForObject(
                      new RawParameterizedSqlStatement(
                          sql.toString(),
                          column.getSchema().getName(),
                          column.getRelation().getName(),
                          column.getName()),
                      String.class);

          dataType = dataType.replace("character varying", "varchar");
          dataType = dataType.replace("timestamp with time zone", "timestamptz");
          dataType = dataType.replace("double precision", "float8");
          dataType = dataType.replace("double precision[]", "float8[]");
          return new DataType(dataType);
        } catch (DatabaseException e) {
          Scope.getCurrentScope().getLog(getClass()).warning("Error fetching data type column", e);
        }
      }
    }
    return super.readDataType(columnMetadataResultSet, column, database);
  }

  @Override
  public Class<? extends SnapshotGenerator>[] replaces() {
    return new Class[] {ColumnSnapshotGenerator.class};
  }
}
