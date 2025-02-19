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
package liquibase.ext.spanner.change;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import liquibase.change.AddColumnConfig;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddColumnChange;
import liquibase.change.core.DropColumnChange;
import liquibase.change.core.MergeColumnChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Column;

/**
 * Cloud Spanner-specific implementation of {@link MergeColumnChange}. Cloud Spanner requires all
 * UPDATE and DELETE statements to include a WHERE clause, even when all rows should be
 * updated/deleted. This feature is a safety precaution against accidental updates/deletes.
 *
 * <p>{@link SpannerMergeColumnsChange} will use a Partitioned DML statement to fill the data in the
 * new column.
 */
@DatabaseChange(
    name = "mergeColumns",
    description =
        "Concatenates the values in two columns, joins them by with string, and stores the resulting value in a new column.",
    priority = ChangeMetaData.PRIORITY_DATABASE)
public class MergeColumnsChangeSpanner extends MergeColumnChange {

  @Override
  public SqlStatement[] generateStatements(final Database database) {
    List<SqlStatement> statements = new ArrayList<>();

    AddColumnChange addNewColumnChange = new AddColumnChange();
    addNewColumnChange.setCatalogName(getCatalogName());
    addNewColumnChange.setSchemaName(getSchemaName());
    addNewColumnChange.setTableName(getTableName());
    final AddColumnConfig columnConfig = new AddColumnConfig();
    columnConfig.setName(getFinalColumnName());
    columnConfig.setType(getFinalColumnType());
    addNewColumnChange.addColumn(columnConfig);
    statements.addAll(Arrays.asList(addNewColumnChange.generateStatements(database)));

    statements.add(new RawSqlStatement("SET AUTOCOMMIT=TRUE"));
    statements.add(new RawSqlStatement("SET AUTOCOMMIT_DML_MODE='PARTITIONED_NON_ATOMIC'"));
    String updateStatement =
        "UPDATE "
            + database.escapeTableName(getCatalogName(), getSchemaName(), getTableName())
            + " SET "
            + database.escapeObjectName(getFinalColumnName(), Column.class)
            + " = "
            + database.getConcatSql(
                database.escapeObjectName(getColumn1Name(), Column.class),
                "'" + getJoinString() + "'",
                database.escapeObjectName(getColumn2Name(), Column.class))
            + " WHERE TRUE";
    statements.add(new RawSqlStatement(updateStatement));
    statements.add(new RawSqlStatement("SET AUTOCOMMIT_DML_MODE='TRANSACTIONAL'"));

    DropColumnChange dropColumn1Change = new DropColumnChange();
    dropColumn1Change.setCatalogName(getCatalogName());
    dropColumn1Change.setSchemaName(getSchemaName());
    dropColumn1Change.setTableName(getTableName());
    dropColumn1Change.setColumnName(getColumn1Name());
    statements.addAll(Arrays.asList(dropColumn1Change.generateStatements(database)));

    DropColumnChange dropColumn2Change = new DropColumnChange();
    dropColumn2Change.setCatalogName(getCatalogName());
    dropColumn2Change.setSchemaName(getSchemaName());
    dropColumn2Change.setTableName(getTableName());
    dropColumn2Change.setColumnName(getColumn2Name());
    statements.addAll(Arrays.asList(dropColumn2Change.generateStatements(database)));

    return statements.toArray(new SqlStatement[statements.size()]);
  }
}
