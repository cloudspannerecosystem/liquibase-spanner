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
 * limitations under the License. package liquibase.ext.spanner;
 */
package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.NVarcharType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps NVARCHAR(n) to dialect-specific string types: - STRING(n) or STRING(MAX) for GoogleSQL
 * dialect - varchar(n) or varchar for PostgreSQL dialect
 *
 * <p>This mapping ensures proper handling of variable-length Unicode strings in Cloud Spanner
 * depending on the active dialect.
 */
public class NVarcharTypeSpanner extends NVarcharType {

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
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
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
