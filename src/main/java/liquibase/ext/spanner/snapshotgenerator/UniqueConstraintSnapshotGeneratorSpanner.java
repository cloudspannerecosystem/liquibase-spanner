package liquibase.ext.spanner.snapshotgenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.spanner.CloudSpanner;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.UniqueConstraintSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import java.util.logging.Logger;

public class UniqueConstraintSnapshotGeneratorSpanner extends UniqueConstraintSnapshotGenerator
{
  public UniqueConstraintSnapshotGeneratorSpanner() {
  }

  /*
   * This generator will be in all chains relating to CloudSpanner, whether or not
   * the objectType is UniqueConstraint.
   */
  @Override
  public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    if (database instanceof CloudSpanner) {
      return PRIORITY_DATABASE;
    }
    return PRIORITY_NONE;
  }

  /*
   * If there is a UniqueConstraintSnapshotGenerator in the chain, we replace it. Otherwise
   * the chain will execute like normal.
   */
  @Override
  public Class<? extends SnapshotGenerator>[] replaces() {
    return new Class[]{ UniqueConstraintSnapshotGenerator.class };
  };

  /*
   * Override the normal UniqueConstraint behaviour to return no rows from the database --
   * all behaviour stays the same.
   */
  protected List<CachedRow> listConstraints(
      Table table,
      DatabaseSnapshot snapshot,
      Schema schema) throws DatabaseException, SQLException {
    return new ArrayList<CachedRow>();
  }
}
