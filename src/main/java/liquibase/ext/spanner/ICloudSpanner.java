package liquibase.ext.spanner;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;

import java.sql.SQLException;

public interface ICloudSpanner extends Database {
  Dialect getDialect() throws SQLException;
}
