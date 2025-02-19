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

import liquibase.database.Database;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddForeignKeyConstraintGenerator;
import liquibase.statement.core.AddForeignKeyConstraintStatement;

public class AddForeignKeyConstraintGeneratorSpanner extends AddForeignKeyConstraintGenerator {

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(AddForeignKeyConstraintStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }

  @Override
  public Sql[] generateSql(
      AddForeignKeyConstraintStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    String onUpdate = statement.getOnUpdate();
    String onDelete = statement.getOnDelete();
    try {
      // Ignore any onUpdate or onDelete clauses. Ignoring this instead of failing is
      // consistent with other databases that also do not support these features.
      statement.setOnUpdate(null);
      statement.setOnDelete(null);
      return super.generateSql(statement, database, sqlGeneratorChain);
    } finally {
      statement.setOnUpdate(onUpdate);
      statement.setOnDelete(onDelete);
    }
  }
}
