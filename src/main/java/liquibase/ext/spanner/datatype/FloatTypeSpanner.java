package liquibase.ext.spanner.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.FloatType;
import liquibase.ext.spanner.ICloudSpanner;

public class FloatTypeSpanner extends FloatType {
  private static final DatabaseDataType FLOAT64 = new DatabaseDataType("FLOAT64");

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
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
