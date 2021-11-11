/**
 * Copyright 2021 Google LLC
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

import liquibase.datatype.LiquibaseDataType;
import liquibase.statement.ColumnConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateTableStatement;

/**
 * Liquibase always adds a {@link NotNullConstraint} for all primary key columns, although Cloud
 * Spanner does support nullable columns in the primary key. This {@link CreateTableStatement}
 * overrides that behavior by removing the automatically added {@link NotNullConstraint} again from
 * the primary key columns that are added.
 */
public class CreateTableStatementSpanner extends CreateTableStatement {

  public CreateTableStatementSpanner(String catalogName, String schemaName, String tableName, String remarks) {
    super(catalogName, schemaName, tableName, remarks);
  }
  
  @Override
  public CreateTableStatement addPrimaryKeyColumn(String columnName, LiquibaseDataType columnType, Object defaultValue, String keyName,
      String tablespace, ColumnConstraint... constraints) {
    CreateTableStatement statement = super.addPrimaryKeyColumn(columnName, columnType, defaultValue, keyName, tablespace, constraints);
    if (!containsNotNullConstraintForColumn(columnName, constraints)) {
      getNotNullColumns().remove(columnName);
    }
    return statement;
  }
  
  @Override
  public CreateTableStatement addPrimaryKeyColumn(String columnName, LiquibaseDataType columnType, Object defaultValue,
      Boolean validate,String keyName, String tablespace, ColumnConstraint... constraints) {
    CreateTableStatement statement = super.addPrimaryKeyColumn(columnName, columnType, defaultValue, validate, keyName, tablespace, constraints);
    if (!containsNotNullConstraintForColumn(columnName, constraints)) {
      getNotNullColumns().remove(columnName);
    }
    return statement;
  }

  @Override
  public CreateTableStatement addPrimaryKeyColumn(String columnName, LiquibaseDataType columnType, Object defaultValue,
      Boolean validate, boolean deferrable, boolean initiallyDeferred, String keyName, String tablespace, ColumnConstraint... constraints) {
    CreateTableStatement statement = super.addPrimaryKeyColumn(columnName, columnType, defaultValue, validate, deferrable, initiallyDeferred, keyName, tablespace, constraints);
    if (!containsNotNullConstraintForColumn(columnName, constraints)) {
      getNotNullColumns().remove(columnName);
    }
    return statement;
  }

    private boolean containsNotNullConstraintForColumn(String column, ColumnConstraint... constraints) {
    for (ColumnConstraint constraint : constraints) {
      if (constraint instanceof NotNullConstraint) {
        NotNullConstraint nn = (NotNullConstraint) constraint;
        if (column.equals(nn.getColumnName())) {
          return true;
        }
      }
    }
    return false;
  }
}
