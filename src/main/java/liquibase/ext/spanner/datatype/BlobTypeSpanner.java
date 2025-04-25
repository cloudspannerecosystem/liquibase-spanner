package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.BlobType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps BLOB to the appropriate type for Cloud Spanner: - BYTES(MAX) for GoogleSQL dialect - bytea
 * for PostgreSQL dialect
 */
public class BlobTypeSpanner extends BlobType {
  private static final DatabaseDataType BLOB = new DatabaseDataType("BYTES(MAX)");
  private static final DatabaseDataType BYTEA = new DatabaseDataType("bytea");

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
    if (database instanceof ICloudSpanner) {
      Dialect dialect = ((ICloudSpanner) database).getDialect();
      return dialect == Dialect.POSTGRESQL ? BYTEA : BLOB;
    } else {
      return super.toDatabaseDataType(database);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }
}
