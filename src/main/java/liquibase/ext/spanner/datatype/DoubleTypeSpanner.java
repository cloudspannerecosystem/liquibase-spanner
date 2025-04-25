package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.DoubleType;
import liquibase.ext.spanner.ICloudSpanner;

public class DoubleTypeSpanner extends DoubleType {
  private static final DatabaseDataType FLOAT64 = new DatabaseDataType("FLOAT64");
  private static final DatabaseDataType DOUBLE = new DatabaseDataType("float8");

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      return dialect == Dialect.POSTGRESQL ? DOUBLE : FLOAT64;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
