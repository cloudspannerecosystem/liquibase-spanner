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
package liquibase.ext.spanner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddForeignKeyConstraintChange;
import liquibase.change.core.AddLookupTableChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Column;

@DatabaseChange(
    name = "addLookupTable",
    description =
        "Creates a lookup table containing values stored in a column and creates a foreign key to the new table.",
    priority = ChangeMetaData.PRIORITY_DATABASE,
    appliesTo = "column")
public class AddLookupTableChangeSpanner extends AddLookupTableChange {

  @Override
  public boolean supports(Database database) {
    return (database instanceof CloudSpanner);
  }

  public SqlStatement[] generateStatements(Database database) {
    List<SqlStatement> statements = new ArrayList<>();

    String newTableCatalogName = getNewTableCatalogName();
    String newTableSchemaName = getNewTableSchemaName();

    String existingTableCatalogName = getExistingTableCatalogName();
    String existingTableSchemaName = getExistingTableSchemaName();

    SqlStatement[] createTablesSQL =
        new SqlStatement[] {
          new RawSqlStatement(
              "CREATE TABLE "
                  + database.escapeTableName(
                      newTableCatalogName, newTableSchemaName, getNewTableName())
                  + " ("
                  + database.escapeObjectName(getNewColumnName(), Column.class)
                  + " "
                  + getNewColumnDataType()
                  + " NOT NULL) PRIMARY KEY ("
                  + database.escapeObjectName(getNewColumnName(), Column.class)
                  + ")"),
          new RawSqlStatement(
              "INSERT INTO "
                  + database.escapeTableName(
                      newTableCatalogName, newTableSchemaName, getNewTableName())
                  + " ("
                  + database.escapeObjectName(getNewColumnName(), Column.class)
                  + ") SELECT DISTINCT "
                  + database.escapeObjectName(getExistingColumnName(), Column.class)
                  + " FROM "
                  + database.escapeTableName(
                      existingTableCatalogName, existingTableSchemaName, getExistingTableName())
                  + " WHERE "
                  + database.escapeObjectName(getExistingColumnName(), Column.class)
                  + " IS NOT NULL"),
        };
    statements.addAll(Arrays.asList(createTablesSQL));

    AddForeignKeyConstraintChange addFKChange = new AddForeignKeyConstraintChange();
    addFKChange.setBaseTableSchemaName(existingTableSchemaName);
    addFKChange.setBaseTableName(getExistingTableName());
    addFKChange.setBaseColumnNames(getExistingColumnName());
    addFKChange.setReferencedTableSchemaName(newTableSchemaName);
    addFKChange.setReferencedTableName(getNewTableName());
    addFKChange.setReferencedColumnNames(getNewColumnName());

    addFKChange.setConstraintName(getFinalConstraintName());
    statements.addAll(Arrays.asList(addFKChange.generateStatements(database)));

    return statements.toArray(new SqlStatement[statements.size()]);
  }
}
