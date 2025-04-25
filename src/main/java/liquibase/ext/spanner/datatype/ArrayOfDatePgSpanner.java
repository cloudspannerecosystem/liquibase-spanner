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
import liquibase.datatype.core.UnknownType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps date[] to dialect-specific array types: - ARRAY<DATE> for GoogleSQL dialect - date[] for
 * PostgreSQL dialect
 */
@DataTypeInfo(
    name = "date[]",
    aliases = {"java.sql.Types.ARRAY", "java.lang.Date[]"},
    minParameters = 0,
    maxParameters = 0,
    priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ArrayOfDatePgSpanner extends UnknownType {
  public ArrayOfDatePgSpanner() {
    super("date[]", 0, 0);
  }

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {

    Dialect dialect = ((ICloudSpanner) database).getDialect();
    return new DatabaseDataType(dialect == Dialect.POSTGRESQL ? "date[]" : "ARRAY<DATE>");
  }
}
