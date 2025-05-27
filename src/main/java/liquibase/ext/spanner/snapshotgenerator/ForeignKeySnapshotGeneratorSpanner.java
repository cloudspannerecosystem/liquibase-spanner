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
package liquibase.ext.spanner.snapshotgenerator;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.ForeignKey;

public class ForeignKeySnapshotGeneratorSpanner extends ForeignKeySnapshotGenerator {
  /**
   * This generator will be in all chains relating to CloudSpanner, whether or not the objectType is
   * {@link ForeignKey}.
   */
  @Override
  public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    if (database instanceof ICloudSpanner) {
      return PRIORITY_DATABASE;
    }
    return PRIORITY_NONE;
  }

  @Override
  public DatabaseObject snapshot(
      DatabaseObject example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain)
      throws DatabaseException, InvalidExampleException {
    // Skip foreign keys that are actually INTERLEAVED TABLE relationships.
    // These are the only foreign keys in Cloud Spanner without a name.
    if (example instanceof ForeignKey && example.getName() == null) {
      return null;
    }
    return super.snapshot(example, snapshot, chain);
  }

  /**
   * If there is a {@link ForeignKeySnapshotGenerator} in the chain, we replace it. Otherwise the
   * chain will execute like normal.
   */
  @Override
  public Class<? extends SnapshotGenerator>[] replaces() {
    return new Class[] {ForeignKeySnapshotGenerator.class};
  }
}
