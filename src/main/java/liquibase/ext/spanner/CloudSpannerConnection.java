/**
 * Copyright 2021 Google LLC
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.spanner;

import com.google.cloud.spanner.jdbc.JdbcDriver;
import java.sql.Connection;
import java.sql.Driver;
import java.util.Properties;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

public class CloudSpannerConnection extends JdbcConnection {
  private final DatabaseConnection originalConnection;

  public CloudSpannerConnection() {
    this.originalConnection = null;
  }

  public CloudSpannerConnection(Connection connection) {
    super(connection);
    this.originalConnection = null;
  }

  CloudSpannerConnection(Connection connection, DatabaseConnection originalConnection) {
    super(connection);
    this.originalConnection = originalConnection;
  }

  public int getPriority() {
    // Using PRIORITY_DATABASE here does not work, as Liquibase core contains ProJdbcConnection that
    // already has that priority.
    return Integer.MAX_VALUE;
  }

  @Override
  public void close() throws DatabaseException {
    // Also close the original connection that this connection replaced.
    if (originalConnection != null && !originalConnection.isClosed()) {
      originalConnection.close();
    }
    super.close();
  }

  @Override
  public void open(String url, Driver driverObject, Properties driverProperties)
      throws DatabaseException {
    // This method is called by Liquibase when a new connection is needed. The connection URL is
    // part of the configuration that a user specifies for Liquibase. We append a user-agent string
    // to the connection URL so we can track the usage of Liquibase.
    if (url.startsWith("jdbc:cloudspanner") && !url.contains("userAgent=")) {
      if (driverObject instanceof JdbcDriver) {
        JdbcDriver driver = (JdbcDriver) driverObject;
        // Only version 2 and higher support the Liquibase user-agent string.
        // Earlier versions do support Liquibase, but will just use the default
        // user-agent string.
        if (driver.getMajorVersion() >= 2) {
          url = url + ";userAgent=sp-liq";
        }
      }
    }
    super.open(url, driverObject, driverProperties);
  }
}
