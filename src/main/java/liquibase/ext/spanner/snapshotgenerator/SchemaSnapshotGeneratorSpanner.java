package liquibase.ext.spanner.snapshotgenerator;

import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.snapshot.jvm.ForeignKeySnapshotGenerator;
import liquibase.snapshot.jvm.SchemaSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;

public class SchemaSnapshotGeneratorSpanner extends SchemaSnapshotGenerator {
    /**
     * This generator will be in all chains relating to CloudSpanner, whether or not
     * the objectType is {@link Schema}.
     */
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof ICloudSpanner) {
            return PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (example instanceof Schema) {
            Schema schema = (Schema) example;
            schema.getAttributes().remove("catalog");
            return schema;
        }
        return super.snapshotObject(example, snapshot);
    }
    /**
     * If there is a {@link SchemaSnapshotGenerator} in the chain, we replace it. Otherwise
     * the chain will execute like normal.
     */
    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[]{ SchemaSnapshotGenerator.class };
    };
}
