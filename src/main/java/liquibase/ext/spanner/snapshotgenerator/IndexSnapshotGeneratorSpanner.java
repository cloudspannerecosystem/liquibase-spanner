/**
 * Copyright 2023 Google LLC
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

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.exception.DatabaseException;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.snapshot.jvm.IndexSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.StringUtil;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Analyses the properties of a database index and creates an object representation ("snapshot").
 */
public class IndexSnapshotGeneratorSpanner extends IndexSnapshotGenerator {

    /**
     * This generator will be in all chains that import the Cloud Spanner provider, also if it is used in combination with other databases.
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
        Database database = snapshot.getDatabase();
        Relation exampleIndex = ((Index) example).getRelation();

        String tableName = null;
        Schema schema = null;
        if (exampleIndex != null) {
            tableName = exampleIndex.getName();
            schema = exampleIndex.getSchema();
        }

        if (schema == null) {
            schema = new Schema(database.getDefaultCatalogName(), database.getDefaultSchemaName());
        }


        for (int i = 0; i < ((Index) example).getColumns().size(); i++) {
            ((Index) example).getColumns().set(i, ((Index) example).getColumns().get(i));
        }

        String exampleName = example.getName();
        if (exampleName != null) {
            exampleName = database.correctObjectName(exampleName, Index.class);
        }

        Map<String, Index> foundIndexes = new HashMap<>();
        JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = null;
        List<CachedRow> rs = null;
        try {
            databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaDataFromCache();

            rs = databaseMetaData.getIndexInfo(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), tableName, exampleName);

            for (CachedRow row : rs) {
                String rawIndexName = row.getString("INDEX_NAME");
                String indexName = cleanNameFromDatabase(rawIndexName, database);
                String correctedIndexName = database.correctObjectName(indexName, Index.class);

                if (indexName == null) {
                    continue;
                }
                if ((exampleName != null) && !exampleName.equals(correctedIndexName)) {
                    continue;
                }
                Short type = row.getShort("TYPE");
                Boolean nonUnique = row.getBoolean("NON_UNIQUE");
                if (nonUnique == null) {
                    nonUnique = true;
                }

                String columnName = cleanNameFromDatabase(row.getString("COLUMN_NAME"), database);
                Short position = row.getShort("ORDINAL_POSITION");
                String definition = StringUtil.trimToNull(row.getString("FILTER_CONDITION"));

                // Have we already seen/found this index? If not, let's read its properties!
                Index returnIndex = foundIndexes.get(correctedIndexName);
                if (returnIndex == null) {
                    returnIndex = new Index();
                    Relation relation = new Table();
                    if ("V".equals(row.getString("INTERNAL_OBJECT_TYPE"))) {
                        relation = new View();
                    }
                    returnIndex.setRelation(relation.setName(row.getString("TABLE_NAME")).setSchema(schema));
                    returnIndex.setName(indexName);
                    returnIndex.setUnique(!nonUnique);

                    String tablespaceName = row.getString("TABLESPACE_NAME");
                    if ((tablespaceName != null) && database.supportsTablespaces()) {
                        returnIndex.setTablespace(tablespaceName);
                    }

                    if (type == DatabaseMetaData.tableIndexClustered) {
                        returnIndex.setClustered(true);
                    }
                    foundIndexes.put(correctedIndexName, returnIndex);
                }

                if (position == null) {
                    List<String> includedColumns = returnIndex.getAttribute("includedColumns", List.class);
                    if (includedColumns == null) {
                        includedColumns = new ArrayList<>();
                        returnIndex.setAttribute("includedColumns", includedColumns);
                    }
                    includedColumns.add(columnName);
                } else {
                    if (position != 0) { //if really a column, position is 1-based.
                        for (int i = returnIndex.getColumns().size(); i < position; i++) {
                            returnIndex.getColumns().add(null);
                        }

                        // Is this column a simple column (definition == null)
                        // or is it a computed expression (definition != null)
                        if (definition == null) {
                            String ascOrDesc;
                            if (database instanceof Db2zDatabase) {
                                ascOrDesc = row.getString("ORDER");
                            } else {
                                ascOrDesc = row.getString("ASC_OR_DESC");
                            }
                            Boolean descending = "D".equals(ascOrDesc) ? Boolean.TRUE : ("A".equals(ascOrDesc) ?
                                Boolean.FALSE : null);
                            returnIndex.getColumns().set(position - 1, new Column(columnName)
                                .setDescending(descending).setRelation(returnIndex.getRelation()));
                        } else {
                            returnIndex.getColumns().set(position - 1, new Column()
                                .setRelation(returnIndex.getRelation()).setName(definition, true));
                        }
                    }
                }
            }

        } catch (Exception e) {
            throw new DatabaseException(e);
        }

        if (exampleName != null) {
            return foundIndexes.get(exampleName);
        } else {
            //prefer clustered version of the index
            List<Index> nonClusteredIndexes = new ArrayList<>();
            for (Index index : foundIndexes.values()) {
                if (DatabaseObjectComparatorFactory.getInstance().isSameObject(index.getRelation(), exampleIndex, snapshot.getSchemaComparisons(), database)) {
                    boolean actuallyMatches = false;
                    if (database.isCaseSensitive()) {
                        if (index.getColumnNames().equals(((Index) example).getColumnNames())) {
                            actuallyMatches = true;
                        }
                    } else {
                        if (index.getColumnNames().equalsIgnoreCase(((Index) example).getColumnNames())) {
                            actuallyMatches = true;
                        }
                    }
                    if (actuallyMatches) {
                        if ((index.getClustered() != null) && index.getClustered()) {
                            return finalizeIndex(schema, tableName, index, snapshot);
                        } else {
                            nonClusteredIndexes.add(index);
                        }
                    }
                }
            }
            if (!nonClusteredIndexes.isEmpty()) {
                return finalizeIndex(schema, tableName, nonClusteredIndexes.get(0), snapshot);
            }
            return null;
        }
    }
}
