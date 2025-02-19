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
package liquibase.ext.spanner.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.UnknownType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * ARRAY<STRING(len)> needs special handling because it contains a length parameter that is not at
 * the end of the type definition.
 */
@DataTypeInfo(
    name = "array<string>",
    aliases = {"java.sql.Types.ARRAY", "java.lang.String[]"},
    minParameters = 1,
    maxParameters = 1,
    priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ArrayOfStringSpanner extends UnknownType {
  public ArrayOfStringSpanner() {
    super("ARRAY<STRING>", 1, 1);
  }

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    Object[] parameters = getParameters();
    if (parameters != null && parameters.length == 1) {
      return new DatabaseDataType(String.format("ARRAY<STRING(%s)>", parameters[0]));
    }
    return super.toDatabaseDataType(database);
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
