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
package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.statement.DatabaseFunction;

/**
 * Custom Liquibase data type for handling STRING types in Google Cloud Spanner, supporting both
 * GoogleSQL and PostgreSQL dialects.
 */
@DataTypeInfo(
    name = "STRING",
    aliases = {"java.sql.Types.Varchar", "java.lang.String"},
    minParameters = 1,
    maxParameters = 1,
    priority = LiquibaseDataType.PRIORITY_DATABASE)
public class StringTypeSpanner extends LiquibaseDataType {

  public StringTypeSpanner() {
    super("STRING", 1, 1);
  }

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
    return LoadDataChange.LOAD_DATA_TYPE.STRING;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      Object[] params = getParameters();

      if (dialect == Dialect.POSTGRESQL) {
        return (params != null && params.length > 0)
            ? new DatabaseDataType("varchar(" + params[0] + ")")
            : new DatabaseDataType("varchar");
      } else {
        return (params != null && params.length > 0)
            ? new DatabaseDataType("STRING(" + params[0] + ")")
            : new DatabaseDataType("STRING(MAX)");
      }
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public String objectToSql(Object value, Database database) {
    if (value instanceof DatabaseFunction) {
      return super.objectToSql(value, database);
    } else {
      return "'" + super.objectToSql(value, database) + "'";
    }
  }
}
