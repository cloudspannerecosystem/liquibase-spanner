/**
 * Copyright 2020 Google LLC
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

import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import java.sql.DriverManager;
import java.sql.SQLException;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;

public class CloudSpanner extends AbstractJdbcDatabase implements ICloudSpanner {

  public CloudSpanner() {
    unmodifiableDataTypes.add(Type.Code.BOOL.name().toLowerCase());
    unmodifiableDataTypes.add(Type.Code.DATE.name().toLowerCase());
    unmodifiableDataTypes.add(Type.Code.FLOAT64.name().toLowerCase());
    unmodifiableDataTypes.add(Type.Code.INT64.name().toLowerCase());
    unmodifiableDataTypes.add(Type.Code.NUMERIC.name().toLowerCase());
    unmodifiableDataTypes.add(Type.Code.STRUCT.name().toLowerCase());
    unmodifiableDataTypes.add(Type.Code.TIMESTAMP.name().toLowerCase());
  }

  @Override
  public java.lang.Integer getDefaultPort() {
    return Integer.valueOf(9010);
  }
  
  @Override
  public boolean dataTypeIsNotModifiable(final String typeName) {
    // All data types are returned including the length by the JDBC driver
    // and are therefore unmodifiable.
    return true;
  }

  @Override
  public String getDateLiteral(final String isoDate) {
    String literal = super.getDateLiteral(isoDate);
    if (isDateTime(isoDate)) {
      literal = "TIMESTAMP " + literal.replace(' ', 'T');
    } else {
      literal = "DATE " + literal;
    }
    return literal;
  }

  @Override
  public String getCurrentDateTimeFunction() {
    return "CURRENT_TIMESTAMP()";
  }

  @Override
  public String getShortName() {
    return "cloudspanner";
  }

  @Override
  public String getDefaultDatabaseProductName() {
    return "cloudspanner";
  }

  @Override
  public boolean supportsTablespaces() {
    return false;
  }

  @Override
  public String getDefaultDriver(String url) {
    if (url != null && url.startsWith("jdbc:cloudspanner:")) {
      return "com.google.cloud.spanner.jdbc.JdbcDriver";
    }
    return null;
  }

  protected String getConnectionSchemaName() {
    if (getConnection() == null) {
      return null;
    }
    return defaultSchemaName;
  }

  @Override
  public void setConnection(final DatabaseConnection conn) {
    DatabaseConnection connectionToUse = conn;
    // If a user creates a JDBC connection manually and then passes this manually into Liquibase,
    // then this method will be called by Liquibase. Normally, a connection will be opened by
    // Liquibase based on the connection URL that is configured. In that case, the
    // CloudSpannerConnection class will ensure that the correct user-agent string is set. That is
    // not the case when a user creates the connection programmatically and passes it in to
    // Liquibase. This method therefore checks whether it is actually a Spanner JDBC connection,
    // and if it is, replaces it with a new connection that uses the same connection URL + the
    // user-agent string. The original connection is kept open in case the caller also uses the
    // connection for other purposes, but will automatically be closed when the 'replacement'
    // connection is closed.
    // The latter should be safe, even if the caller uses the connection for other purposes, as
    // even if the connection was not replaced it would have been closed by Liquibase at the same
    // moment.
    if (!(conn instanceof CloudSpannerConnection) && conn instanceof JdbcConnection
        && ((JdbcConnection) conn)
            .getUnderlyingConnection() instanceof CloudSpannerJdbcConnection) {
      // The underlying connection is a Spanner JDBC connection. Check whether it already included a
      // user-agent string.
      if (!conn.getURL().contains("userAgent=")) {
        // The underlying connection does not use a specific user-agent string. Create a replacement
        // connection that will be used by Liquibase with the correct user-agent.
        try {
          connectionToUse = new CloudSpannerConnection(
              DriverManager.getConnection(conn.getURL() + ";userAgent=sp-liq"), conn);
        } catch (SQLException e) {
          // Ignore and use the original connection. This could for example happen if the user is
          // using an older version of the Spanner JDBC driver that does not support this user-agent
          // string.
        }
      }
    }
    super.setConnection(connectionToUse);
  }

  @Override
  public boolean supportsInitiallyDeferrableColumns() {
    return false;
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean requiresUsername() {
    return false;
  }

  @Override
  public boolean requiresPassword() {
    return false;
  }

  @Override
  public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) {
    return conn.getURL().contains("cloudspanner");
  }

  @Override
  public boolean supportsAutoIncrement() {
    return false;
  }

  @Override
  public boolean supportsSequences() {
    return false;
  }

  @Override
  public boolean supportsCatalogs() {
    return false;
  }

  @Override
  public boolean isCaseSensitive() {
    return false;
  }

  @Override
  public boolean supportsSchemas() {
    return false;
  }

  @Override
  public boolean supportsRestrictForeignKeys() {
    return false;
  }

  @Override
  public boolean canCreateChangeLogTable() {
    return false;
  }

  @Override
  public boolean supportsDDLInTransaction() {
    return false;
  }

  @Override
  public boolean supportsPrimaryKeyNames() {
    return false;
  }

  @Override
  protected String getQuotingStartCharacter() {
    return "`";
  }

  @Override
  protected String getQuotingEndCharacter() {
    return "`";
  }

  @Override
  protected String getQuotingEndReplacement() {
    return "\\`";
  }

  @Override
  public String escapeStringForDatabase(String string) {
    return string == null ? null : string.replace("'", "\\'");
  }
}
