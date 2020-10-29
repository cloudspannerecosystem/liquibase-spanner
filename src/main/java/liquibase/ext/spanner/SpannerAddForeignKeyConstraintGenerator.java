package liquibase.ext.spanner;

import liquibase.database.Database;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddForeignKeyConstraintGenerator;
import liquibase.statement.core.AddForeignKeyConstraintStatement;

public class SpannerAddForeignKeyConstraintGenerator extends AddForeignKeyConstraintGenerator {
  static class NamelessForeignKeyConstraint extends AddForeignKeyConstraintStatement {
    public NamelessForeignKeyConstraint(AddForeignKeyConstraintStatement delegate) {
      super(
          delegate.getConstraintName(),
          delegate.getBaseTableCatalogName(),
          delegate.getBaseTableSchemaName(),
          delegate.getBaseTableName(),
          delegate.getBaseColumns(),
          delegate.getReferencedTableCatalogName(),
          delegate.getReferencedTableSchemaName(),
          delegate.getReferencedTableName(),
          delegate.getReferencedColumns());
    }

    @Override
    public String getConstraintName() {
      return "";
    }
  }

  @Override
  public boolean supports(AddForeignKeyConstraintStatement statement, Database database) {
    return database instanceof CloudSpanner;
  }

  @Override
  public Sql[] generateSql(
      AddForeignKeyConstraintStatement statement,
      Database database,
      @SuppressWarnings("rawtypes") SqlGeneratorChain sqlGeneratorChain) {
    return super.generateSql(
        new NamelessForeignKeyConstraint(statement), database, sqlGeneratorChain);
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
