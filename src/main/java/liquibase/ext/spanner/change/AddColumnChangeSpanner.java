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
package liquibase.ext.spanner.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddColumnChange;
import liquibase.database.Database;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.UpdateStatement;

@DatabaseChange(
    name = "addColumn",
    description = "Adds a new column to an existing table",
    priority = ChangeMetaData.PRIORITY_DATABASE,
    appliesTo = "table")
public class AddColumnChangeSpanner extends AddColumnChange {

  @Override
  public boolean supports(Database database) {
    return (database instanceof ICloudSpanner);
  }

  @Override
  public SqlStatement[] generateStatements(Database database) {
    SqlStatement[] statements = super.generateStatements(database);
    for (SqlStatement statement : statements) {
      if (statement instanceof UpdateStatement) {
        UpdateStatement updateStatement = (UpdateStatement) statement;
        if (updateStatement.getWhereClause() == null) {
          updateStatement.setWhereClause("TRUE");
        }
      }
    }
    return statements;
  }
}
