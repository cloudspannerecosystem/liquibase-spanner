package liquibase.ext.spanner.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AlterSequenceGenerator;
import liquibase.statement.core.AlterSequenceStatement;
import liquibase.statement.core.RenameSequenceStatement;

public class AlterSequenceGeneratorSpanner extends AlterSequenceGenerator {
  static final String ALTER_SEQUENCE_VALIDATION_ERROR =
      "The Liquibase Cloud Spanner extension does not support altering sequences from XML. Please use SQL to create the change instead.";

  @Override
  public ValidationErrors validate(
      AlterSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(statement, database, sqlGeneratorChain);
    errors.addError(ALTER_SEQUENCE_VALIDATION_ERROR);
    return errors;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(AlterSequenceStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }
}
