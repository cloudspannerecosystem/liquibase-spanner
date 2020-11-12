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
public class SpannerAddLookupTableChange extends AddLookupTableChange {

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
