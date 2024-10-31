/**
 * Copyright 2024 Google LLC
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
package liquibase.ext.spanner.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DeleteGenerator;
import liquibase.statement.core.DeleteStatement;

public class DeleteGeneratorSpanner extends DeleteGenerator {

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(DeleteStatement statement, Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public Sql[] generateSql(
      DeleteStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    if (statement.getWhere() == null) {
      statement.setWhere("true");
    }
    return super.generateSql(statement, database, sqlGeneratorChain);
  }

}
