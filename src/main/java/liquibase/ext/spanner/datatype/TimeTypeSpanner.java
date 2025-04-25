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
package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.TimeType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Cloud Spanner does not have a data type that only stores time information. The best possible
 * translation is therefore a TIMESTAMP or TIMESTAMPTZ for Postgresql dialect column.
 */
public class TimeTypeSpanner extends TimeType {
  private static final DatabaseDataType TIME = new DatabaseDataType("TIMESTAMP");
  private static final DatabaseDataType TIME_TZ = new DatabaseDataType("timestamptz");

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      return dialect == Dialect.POSTGRESQL ? TIME_TZ : TIME;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
