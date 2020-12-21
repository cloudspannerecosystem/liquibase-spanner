package liquibase.ext.spanner.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.DoubleType;
import liquibase.ext.spanner.CloudSpanner;

public class DoubleTypeSpanner extends DoubleType {
  private static final DatabaseDataType FLOAT64 = new DatabaseDataType("FLOAT64");

  @Override
  public boolean supports(Database database) {
    return database instanceof CloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof CloudSpanner) {
      return FLOAT64;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
