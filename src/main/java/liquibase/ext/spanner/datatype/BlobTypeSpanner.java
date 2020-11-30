package liquibase.ext.spanner;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.BlobType;

/** BLOB is translated to BYTES(MAX) */
public class BlobTypeSpanner extends BlobType {
  private static final DatabaseDataType BLOB = new DatabaseDataType("BYTES(MAX)");

  @Override
  public boolean supports(Database database) {
    return database instanceof CloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof CloudSpanner) {
      return BLOB;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
