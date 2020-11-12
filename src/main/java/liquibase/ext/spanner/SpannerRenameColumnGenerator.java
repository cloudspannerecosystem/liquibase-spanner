package liquibase.ext.spanner;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RenameColumnGenerator;
import liquibase.statement.core.RenameColumnStatement;

public class SpannerRenameColumnGenerator extends RenameColumnGenerator {
  static final String RENAME_COLUMN_VALIDATION_ERROR =
      "Cloud Spanner does not support renaming a column";

  @Override
  public ValidationErrors validate(
      RenameColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(statement, database, sqlGeneratorChain);
    errors.addError(RENAME_COLUMN_VALIDATION_ERROR);
    return errors;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(RenameColumnStatement statement, Database database) {
    return (database instanceof CloudSpanner);
  }
}
