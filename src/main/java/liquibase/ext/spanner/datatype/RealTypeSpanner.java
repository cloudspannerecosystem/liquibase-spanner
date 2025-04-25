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
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.FloatType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps real to dialect-specific floating-point types: - FLOAT32 for GoogleSQL dialect - real for
 * PostgreSQL dialect
 *
 * <p>In GoogleSQL, FLOAT32 is used for single-precision (32-bit) floating-point values. PostgreSQL
 * uses the keyword real for the same purpose.
 */
@DataTypeInfo(
    name = "real",
    aliases = {"java.sql.Types.FLOAT", "java.lang.float"},
    minParameters = 0,
    maxParameters = 0,
    priority = LiquibaseDataType.PRIORITY_DATABASE)
public class RealTypeSpanner extends FloatType {
  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      return new DatabaseDataType(dialect == Dialect.POSTGRESQL ? "real" : "FLOAT32");
    }
    return super.toDatabaseDataType(database);
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
