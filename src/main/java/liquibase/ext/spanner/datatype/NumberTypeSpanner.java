package liquibase.ext.spanner.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.NumberType;
import liquibase.ext.spanner.CloudSpanner;
import liquibase.ext.spanner.ICloudSpanner;

public class NumberTypeSpanner extends NumberType {
  private static final DatabaseDataType NUMERIC = new DatabaseDataType("NUMERIC");

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      return NUMERIC;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
