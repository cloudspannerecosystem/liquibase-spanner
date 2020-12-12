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

import com.google.common.collect.ImmutableList;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.structure.DatabaseObject;

public class SpannerDatabaseSnapshot extends JdbcDatabaseSnapshot {
  
  public class SpannerCachingDatabaseMetaData extends CachingDatabaseMetaData {
    public SpannerCachingDatabaseMetaData(Database database, DatabaseMetaData metaData) {
      super(database, metaData);
    }

    @Override
    public List<CachedRow> getUniqueConstraints(String catalogName, String schemaName,
        String tableName) throws DatabaseException {
      // Cloud Spanner does not support unique constraints.
      return ImmutableList.of();
    }
  }
  
  private SpannerCachingDatabaseMetaData cachingDatabaseMetaData;
  
  public SpannerDatabaseSnapshot(DatabaseObject[] examples, Database database)
      throws DatabaseException, InvalidExampleException {
    super(examples, database);
  }

  public SpannerDatabaseSnapshot(DatabaseObject[] examples, Database database,
      SnapshotControl snapshotControl) throws DatabaseException, InvalidExampleException {
    super(examples, database, snapshotControl);
  }

  @Override
  public SpannerCachingDatabaseMetaData getMetaDataFromCache() throws SQLException {
    if (cachingDatabaseMetaData == null) {
      DatabaseMetaData databaseMetaData = null;
      if (getDatabase().getConnection() != null) {
        databaseMetaData = ((JdbcConnection) getDatabase().getConnection())
            .getUnderlyingConnection().getMetaData();
      }

      cachingDatabaseMetaData = new SpannerCachingDatabaseMetaData(this.getDatabase(), databaseMetaData);
    }
    return cachingDatabaseMetaData;
  }
}
