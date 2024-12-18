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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;

import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import liquibase.ext.spanner.JdbcMetadataQueries.IndexMetaData;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Index;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Sequence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import liquibase.CatalogAndSchema;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.ext.spanner.JdbcMetadataQueries.ColumnMetaData;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.core.Table;
import liquibase.ext.spanner.JdbcMetadataQueries.SequenceMetadata;

@Execution(ExecutionMode.SAME_THREAD)
public class GenerateSnapshotTest extends AbstractMockServerTest {
  
  @BeforeAll
  static void setupResults() {
    mockSpanner.putStatementResult(StatementResult.query(Statement.newBuilder(JdbcMetadataQueries.GET_SCHEMAS)
                .bind("p1").to("%")
            .bind("p2").to("%")
            .build(),
        JdbcMetadataQueries.createGetSchemasResultSet()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_TABLES)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("%") // Table
                .bind("p4")
                .to("TABLE")
                .bind("p5")
                .to("NON_EXISTENT_TYPE") // This is a trick in the JDBC driver to simplify the query
                .build(),
            JdbcMetadataQueries.createGetTablesResultSet(ImmutableList.of("Singers"))));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_TABLES)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("%") // Table
                .bind("p4")
                .to("VIEW")
                .bind("p5")
                .to("NON_EXISTENT_TYPE") // This is a trick in the JDBC driver to simplify the query
                .build(),
            JdbcMetadataQueries.createGetTablesResultSet(ImmutableList.of())));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_PRIMARY_KEYS)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("SINGERS") // Table
                .build(),
            JdbcMetadataQueries.createGetPrimaryKeysResultSet(ImmutableList.of())));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_IMPORTED_KEYS)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("SINGERS") // Table
                .build(),
            JdbcMetadataQueries.createGetImportedKeysResultSet(ImmutableList.of())));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_INDEX_INFO)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("SINGERS") // Table
                .bind("p4")
                .to("%") // Index 
                .bind("p5")
                .to("%") // Unique
                .build(),
            JdbcMetadataQueries.createGetIndexInfoResultSet(ImmutableList.of(
                new IndexMetaData("Singers", false, "Idx_Singers_FirstName", false, 1, "FirstName", true),
                new IndexMetaData("Singers", false, "Idx_Singers_FirstName", false, null, "LastName", null)
            ))));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_COLUMNS)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("%") // Table
                .bind("p4")
                .to("%") // Column 
                .build(),
            JdbcMetadataQueries.createGetColumnsResultSet(
                ImmutableList.of(
                  new ColumnMetaData("Singers", "SingerId", java.sql.Types.BIGINT, "INT64", 8, java.sql.DatabaseMetaData.columnNoNulls),
                  new ColumnMetaData("Singers", "FirstName", java.sql.Types.NVARCHAR, "STRING(100)", 100, java.sql.DatabaseMetaData.columnNullable),
                  new ColumnMetaData("Singers", "LastName", java.sql.Types.NVARCHAR, "STRING(200)", 200, java.sql.DatabaseMetaData.columnNoNulls)
                )
              )
            ));
    mockSpanner.putStatementResult(StatementResult.query(Statement.newBuilder(JdbcMetadataQueries.GET_SCHEMAS)
                .bind("p1").to("%")
                .bind("p2").to("%")
                .build(),
        JdbcMetadataQueries.createGetSchemasResultSet()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_SEQUENCES)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .build(),
            JdbcMetadataQueries.createGetSequenceResultSet(
                ImmutableList.of(
                    new SequenceMetadata("testSequence", "bit_reversed_positive", 100, 5000000, 1)
                )
            )
        ));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testGenerateSnapshot() throws Exception {
    try (Liquibase liquibase = getLiquibase(createConnectionUrl(), "create-snapshot.spanner.yaml")) {
      SnapshotGeneratorFactory factory = SnapshotGeneratorFactory.getInstance();
      Database database = liquibase.getDatabase();
      SnapshotControl control = new SnapshotControl(database);
      CatalogAndSchema schema = new CatalogAndSchema("", "");
      DatabaseSnapshot snapshot = factory.createSnapshot(schema, database, control);
      Set<Schema> schemaSet = snapshot.get(Schema.class);
      assertThat(schemaSet).hasSize(1);
      Schema schemaCatalog = schemaSet.iterator().next();
      // Recursively traverse all types in the snapshot
      verifySnapshotIdsInDatabaseObjects(schemaCatalog, new HashSet<>());
      Set<Table> tables = snapshot.get(Table.class);
      assertThat(tables).hasSize(1);
      Table singers = tables.iterator().next();
      assertThat(singers.getName()).isEqualTo("Singers");
      assertThat(singers.getColumns()).hasSize(3);
      assertThat(singers.getColumn("SingerId").getType().getTypeName()).isEqualTo("INT64");
      assertThat(singers.getColumn("SingerId").getType().toString()).isEqualTo("INT64");
      assertThat(singers.getColumn("FirstName").getType().getTypeName()).isEqualTo("STRING(100)");
      assertThat(singers.getColumn("FirstName").getType().toString()).isEqualTo("STRING(100)");
      assertThat(singers.getColumn("LastName").getType().getTypeName()).isEqualTo("STRING(200)");
      assertThat(singers.getColumn("LastName").getType().toString()).isEqualTo("STRING(200)");

      Set<Index> indexes = snapshot.get(Index.class);
      assertEquals(1, indexes.size());
      Index index = indexes.iterator().next();
      assertEquals("Idx_Singers_FirstName", index.getName());

      Set<Sequence> sequences = snapshot.get(Sequence.class);
      assertEquals(1, sequences.size());
      Sequence sequence = sequences.iterator().next();
      assertEquals("testSequence", sequence.getName());
    }
  }

  private void verifySnapshotIdsInDatabaseObjects(Object object, Set<Object> visited) throws NoSuchFieldException {
    if (object == null) {
      return; // Nothing to traverse
    }
    // Check if the object was already visited
    if (visited.contains(object)) {
      return;
    }
    // Mark the current object as visited
    visited.add(object);

    if (object instanceof DatabaseObject) {
      DatabaseObject dbObject = (DatabaseObject) object;
      //assertThat(dbObject.getSnapshotId()).isNotNull();
      assertWithMessage("Object: " + dbObject.getClass() + " Should have snapshotId")
          .that(dbObject.getSnapshotId()).isNotNull();
      Set<String> attributesObj = dbObject.getAttributes();
      for (String attribute : attributesObj) {
        Object attributesField = dbObject.getAttribute(attribute, new Object());
        if (attributesField instanceof DatabaseObject) {
          verifySnapshotIdsInDatabaseObjects(attributesField, visited);
        } else if (attributesField instanceof Map<?, ?>) {
          if (!((Map<?, ?>) attributesField).isEmpty()) {
            Map<?, ?> attributes = (Map<?, ?>) attributesField;
            attributes.forEach((key, value) -> {
              if (value instanceof Set<?>) {
                ((Set<?>) value).forEach(setValue -> {
                  if (setValue instanceof DatabaseObject) {
                    try {
                      verifySnapshotIdsInDatabaseObjects(setValue, visited);
                    } catch (NoSuchFieldException ignored) {
                    }
                  }
                });
              }
            });
          }
        } else if (attributesField instanceof List<?>) {
          List<?> objectList = (List<?>) attributesField;
          objectList.forEach((value) -> {
            if (value instanceof DatabaseObject) {
              try {
                verifySnapshotIdsInDatabaseObjects(value, visited);
              } catch (NoSuchFieldException ignored) {

              }
            }
          });
        }
      }
    }
  }
}