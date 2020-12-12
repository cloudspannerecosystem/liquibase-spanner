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

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.exception.DatabaseException;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.EmptyDatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.DatabaseObject;

class SpannerSnapshotGeneratorFactory extends SnapshotGeneratorFactory {
  SpannerSnapshotGeneratorFactory() {}

  public DatabaseSnapshot createSnapshot(DatabaseObject[] examples, Database database,
      SnapshotControl snapshotControl) throws DatabaseException, InvalidExampleException {
    if (database instanceof CloudSpanner) {
      DatabaseConnection conn = database.getConnection();
      if (conn == null) {
        return new EmptyDatabaseSnapshot(database, snapshotControl);
      }
      if (conn instanceof OfflineConnection) {
        DatabaseSnapshot snapshot = ((OfflineConnection) conn).getSnapshot(examples);
        if (snapshot == null) {
          throw new DatabaseException("No snapshotFile parameter specified for offline database");
        }
        return snapshot;
      }
      return new SpannerDatabaseSnapshot(examples, database, snapshotControl);
    } else {
      return super.createSnapshot(examples, database, snapshotControl);
    }
  }

}
