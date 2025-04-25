package liquibase.ext.spanner.datatype;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.UnknownType;
import liquibase.ext.spanner.ICloudSpanner;

/**
 * Maps ARRAY<TIMESTAMP> to dialect-specific array types: - ARRAY<TIMESTAMP> for GoogleSQL dialect -
 * timestamptz[] for PostgreSQL dialect
 */
@DataTypeInfo(
    name = "ARRAY<TIMESTAMP>",
    minParameters = 0,
    maxParameters = 0,
    priority = LiquibaseDataType.PRIORITY_DATABASE)
public class ArrayOfTimestampSpanner extends UnknownType {
  public ArrayOfTimestampSpanner() {
    super("ARRAY<TIMESTAMP>", 0, 0);
  }

  @Override
  public boolean supports(Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {

    Dialect dialect = ((ICloudSpanner) database).getDialect();

    if (dialect == Dialect.POSTGRESQL) {
      return new DatabaseDataType("timestamptz[]");
    } else {
      return super.toDatabaseDataType(database);
    }
  }
}
