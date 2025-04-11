package liquibase.ext.spanner;

import com.google.cloud.spanner.Dialect;
import liquibase.database.Database;

public interface ICloudSpanner extends Database {
  Dialect getDialect();
}
