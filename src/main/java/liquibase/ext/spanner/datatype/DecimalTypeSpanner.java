package liquibase.ext.spanner;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.DecimalType;

/** DECIMAL is translated to NUMERIC. */
public class DecimalTypeSpanner extends DecimalType {
  private static final DatabaseDataType DECIMAL = new DatabaseDataType("NUMERIC");

  @Override
  public boolean supports(Database database) {
    return database instanceof CloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof CloudSpanner) {
      return DECIMAL;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
