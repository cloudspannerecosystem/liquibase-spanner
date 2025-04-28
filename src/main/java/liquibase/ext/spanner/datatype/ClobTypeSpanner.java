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
import liquibase.datatype.core.ClobType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps CLOB to dialect-specific types: - STRING(MAX) for GoogleSQL dialect - varchar for PostgreSQL
 * dialect
 *
 * <p>This ensures compatibility with Cloud Spanner's handling of large text fields in both
 * dialects.
 */
public class ClobTypeSpanner extends ClobType {
  private static final DatabaseDataType CLOB = new DatabaseDataType("STRING(MAX)");
  private static final DatabaseDataType CLOB_PG = new DatabaseDataType("varchar");

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      return dialect == Dialect.POSTGRESQL ? CLOB_PG : CLOB;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
