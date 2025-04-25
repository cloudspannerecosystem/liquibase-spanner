package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.UnknownType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps timestamptz[] to dialect-specific array types: - ARRAY<TIMESTAMP> for GoogleSQL dialect -
 * timestamptz[] for PostgreSQL dialect
 */
@DataTypeInfo(
    name = "timestamptz[]",
    minParameters = 0,
    maxParameters = 0,
    priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ArrayOfTimestampPgSpanner extends UnknownType {
  public ArrayOfTimestampPgSpanner() {
    super("timestamptz[]", 0, 0);
  }

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {

    Dialect dialect = ((ICloudSpanner) database).getDialect();
    return new DatabaseDataType(
        dialect == Dialect.POSTGRESQL ? "timestamptz[]" : "ARRAY<TIMESTAMP>");
  }
}
