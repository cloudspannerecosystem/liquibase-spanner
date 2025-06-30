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
import static liquibase.ext.spanner.JdbcMetadataQueries.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.*;
import java.util.*;
import liquibase.CatalogAndSchema;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.ext.spanner.JdbcMetadataQueries.ColumnMetaData;
import liquibase.ext.spanner.JdbcMetadataQueries.IndexMetaData;
import liquibase.ext.spanner.JdbcMetadataQueries.SequenceMetadata;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Index;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Sequence;
import liquibase.structure.core.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class GenerateSnapshotTest extends AbstractMockServerTest {

  @BeforeAll
  static void beforeAll() {
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select view_definition from information_schema.views where table_name='Singers' and table_schema='' and table_catalog=''"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    StructType.Field.newBuilder()
                                        .setName("VIEW_DEFINITION")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING)))))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)))
                .build()));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.of(
                "select view_definition from information_schema.views where table_name='Singers' and table_schema='public' and table_catalog='db_pg'"),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    StructType.Field.newBuilder()
                                        .setName("VIEW_DEFINITION")
                                        .setType(Type.newBuilder().setCode(TypeCode.STRING)))))
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)))
                .build()));
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @ParameterizedTest
  @EnumSource(Dialect.class)
  void testGenerateSnapshot(Dialect dialect) throws Exception {
    String schemaName = dialect == Dialect.POSTGRESQL ? "PUBLIC" : "";
    String catalog = dialect == Dialect.POSTGRESQL ? "DB_PG" : "";
    putMockResultsForSchemas(dialect);
    try (Liquibase liquibase =
        getLiquibase(createConnection(dialect), "create-snapshot.spanner.yaml")) {
      SnapshotGeneratorFactory factory = SnapshotGeneratorFactory.getInstance();
      Database database = liquibase.getDatabase();
      SnapshotControl control = new SnapshotControl(database);
      CatalogAndSchema schema = new CatalogAndSchema(catalog, schemaName);
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
      assertThat(singers.getColumn("SingerId").getType().getTypeName())
          .isEqualTo(dialect == Dialect.POSTGRESQL ? "bigint" : "INT64");
      assertThat(singers.getColumn("SingerId").getType().toString())
          .isEqualTo(dialect == Dialect.POSTGRESQL ? "bigint" : "INT64");
      assertThat(singers.getColumn("FirstName").getType().getTypeName())
          .isEqualTo(dialect == Dialect.POSTGRESQL ? "varchar" : "STRING(100)");
      assertThat(singers.getColumn("FirstName").getType().toString())
          .isEqualTo(dialect == Dialect.POSTGRESQL ? "varchar" : "STRING(100)");
      assertThat(singers.getColumn("LastName").getType().getTypeName())
          .isEqualTo(dialect == Dialect.POSTGRESQL ? "varchar" : "STRING(200)");
      assertThat(singers.getColumn("LastName").getType().toString())
          .isEqualTo(dialect == Dialect.POSTGRESQL ? "varchar" : "STRING(200)");

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

  private void verifySnapshotIdsInDatabaseObjects(Object object, Set<Object> visited)
      throws NoSuchFieldException {
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
      assertThat(dbObject.getSnapshotId()).isNotNull();
      assertWithMessage("Object: " + dbObject.getClass() + " Should have snapshotId")
          .that(dbObject.getSnapshotId())
          .isNotNull();
      Set<String> attributesObj = dbObject.getAttributes();
      for (String attribute : attributesObj) {
        Object attributesField = dbObject.getAttribute(attribute, new Object());
        if (attributesField instanceof DatabaseObject) {
          verifySnapshotIdsInDatabaseObjects(attributesField, visited);
        } else if (attributesField instanceof Map<?, ?>) {
          if (!((Map<?, ?>) attributesField).isEmpty()) {
            Map<?, ?> attributes = (Map<?, ?>) attributesField;
            attributes.forEach(
                (key, value) -> {
                  if (value instanceof Set<?>) {
                    ((Set<?>) value)
                        .forEach(
                            setValue -> {
                              if (setValue instanceof DatabaseObject) {
                                try {
                                  verifySnapshotIdsInDatabaseObjects(setValue, visited);
                                } catch (NoSuchFieldException e) {
                                  fail("Unexpected NoSuchFieldException: " + e.getMessage());
                                }
                              }
                            });
                  }
                });
          }
        } else if (attributesField instanceof List<?>) {
          List<?> objectList = (List<?>) attributesField;
          objectList.forEach(
              (value) -> {
                if (value instanceof DatabaseObject) {
                  try {
                    verifySnapshotIdsInDatabaseObjects(value, visited);
                  } catch (NoSuchFieldException e) {
                    fail("Unexpected NoSuchFieldException: " + e.getMessage());
                  }
                }
              });
        }
      }
    }
  }

  void putMockResultsForSchemas(Dialect dialect) {
    String[] columns = new String[] {"SingerId", "FirstName", "LastName"};
    String schema = dialect == Dialect.POSTGRESQL ? "PUBLIC" : "";
    String catalog = dialect == Dialect.POSTGRESQL ? "DB_PG" : "";
    AbstractStatementParser.ParametersInfo params;
    String sql;
    AbstractStatementParser parser = dialect == Dialect.POSTGRESQL ? PARSER_PG : PARSER;

    sql =
        dialect == Dialect.POSTGRESQL
            ? readSqlFromFile(GET_TABLES, dialect)
            : parser.removeCommentsAndTrim(readSqlFromFile(GET_TABLES, dialect));
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("%") // Table
                .bind("p4")
                .to("VIEW")
                .bind("p5")
                .to("NON_EXISTENT_TYPE") // This is a trick in the JDBC driver to simplify the
                // query
                .build(),
            JdbcMetadataQueries.createGetTablesResultSet(ImmutableList.of("Singers"))));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("%") // Table
                .bind("p4")
                .to("TABLE")
                .bind("p5")
                .to("NON_EXISTENT_TYPE") // This is a trick in the JDBC driver to simplify the
                // query
                .build(),
            JdbcMetadataQueries.createGetTablesResultSet(ImmutableList.of("Singers"))));
    sql =
        dialect == Dialect.POSTGRESQL
            ? readSqlFromFile(GET_PRIMARY_KEYS, dialect)
            : parser.removeCommentsAndTrim(readSqlFromFile(GET_PRIMARY_KEYS, dialect));
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("SINGERS") // Table
                .build(),
            JdbcMetadataQueries.createGetPrimaryKeysResultSet(ImmutableList.of())));
    sql =
        dialect == Dialect.POSTGRESQL
            ? readSqlFromFile(GET_IMPORTED_KEYS, dialect)
            : parser.removeCommentsAndTrim(readSqlFromFile(GET_IMPORTED_KEYS, dialect));
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("SINGERS") // Table
                .build(),
            JdbcMetadataQueries.createGetImportedKeysResultSet(ImmutableList.of())));
    sql =
        dialect == Dialect.POSTGRESQL
            ? readSqlFromFile(GET_INDEX_INFO, dialect)
            : parser.removeCommentsAndTrim(readSqlFromFile(GET_INDEX_INFO, dialect));
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("SINGERS") // Table
                .bind("p4")
                .to("%") // Index
                .bind("p5")
                .to("%") // Unique
                .build(),
            JdbcMetadataQueries.createGetIndexInfoResultSet(
                ImmutableList.of(
                    new IndexMetaData(
                        "Singers", false, "Idx_Singers_FirstName", false, 1, "FirstName", true),
                    new IndexMetaData(
                        "Singers", false, "Idx_Singers_FirstName", false, 2, "LastName", true)))));
    sql =
        dialect == Dialect.POSTGRESQL
            ? readSqlFromFile(GET_COLUMNS, dialect)
            : parser.removeCommentsAndTrim(readSqlFromFile(GET_COLUMNS, dialect));
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schema) // Schema
                .bind("p3")
                .to("%") // Table
                .bind("p4")
                .to("%") // Column
                .build(),
            JdbcMetadataQueries.createGetColumnsResultSet(
                ImmutableList.of(
                    new ColumnMetaData(
                        "Singers",
                        "SingerId",
                        java.sql.Types.BIGINT,
                        "INT64",
                        8,
                        java.sql.DatabaseMetaData.columnNoNulls),
                    new ColumnMetaData(
                        "Singers",
                        "FirstName",
                        java.sql.Types.NVARCHAR,
                        "STRING(100)",
                        100,
                        java.sql.DatabaseMetaData.columnNullable),
                    new ColumnMetaData(
                        "Singers",
                        "LastName",
                        java.sql.Types.NVARCHAR,
                        "STRING(200)",
                        200,
                        java.sql.DatabaseMetaData.columnNoNulls)))));
    sql =
        dialect == Dialect.POSTGRESQL
            ? readSqlFromFile(GET_SCHEMAS, dialect)
            : parser.removeCommentsAndTrim(readSqlFromFile(GET_SCHEMAS, dialect));
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to("%")
                .bind("p2")
                .to("%")
                .build(),
            JdbcMetadataQueries.createGetSchemasResultSet(schema)));
    if (dialect == Dialect.POSTGRESQL) {
      params =
          parser.convertPositionalParametersToNamedParameters(
              '?', JdbcMetadataQueries.GET_SEQUENCES_PG);
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(catalog.toLowerCase()) // Catalog
                  .bind("p2")
                  .to(schema.toLowerCase()) // Schema
                  .build(),
              JdbcMetadataQueries.createGetSequenceResultSet(
                  ImmutableList.of(
                      new SequenceMetadata(
                          "testSequence", "bit_reversed_positive", 100, 5000000, 1)))));
    } else {
      params =
          parser.convertPositionalParametersToNamedParameters(
              '?', JdbcMetadataQueries.GET_SEQUENCES);
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(catalog) // Catalog
                  .bind("p2")
                  .to(schema) // Schema
                  .build(),
              JdbcMetadataQueries.createGetSequenceResultSet(
                  ImmutableList.of(
                      new SequenceMetadata(
                          "testSequence", "bit_reversed_positive", 100, 5000000, 1)))));
    }
    for (String column : columns) {

      params =
          parser.convertPositionalParametersToNamedParameters('?', GET_COLUMN_DEFAULT_STATEMENT);
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(catalog.toLowerCase())
                  .bind("p2")
                  .to(schema.toLowerCase())
                  .bind("p3")
                  .to("Singers")
                  .bind("p4")
                  .to(column)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(
                      ResultSetMetadata.newBuilder()
                          .setRowType(
                              StructType.newBuilder()
                                  .addFields(
                                      StructType.Field.newBuilder()
                                          .setName("COLUMN_DEF")
                                          .setType(
                                              Type.newBuilder().setCode(TypeCode.STRING).build())
                                          .build())
                                  .build())
                          .build())
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                          .build())
                  .build()));

      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(catalog.toLowerCase())
                  .bind("p2")
                  .to(schema)
                  .bind("p3")
                  .to("Singers")
                  .bind("p4")
                  .to(column)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(
                      ResultSetMetadata.newBuilder()
                          .setRowType(
                              StructType.newBuilder()
                                  .addFields(
                                      StructType.Field.newBuilder()
                                          .setName("COLUMN_DEF")
                                          .setType(
                                              Type.newBuilder().setCode(TypeCode.STRING).build())
                                          .build())
                                  .build())
                          .build())
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                          .build())
                  .build()));

      params = parser.convertPositionalParametersToNamedParameters('?', GET_SPANNER_TYPE_STATEMENT);
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(schema.toLowerCase())
                  .bind("p2")
                  .to("Singers")
                  .bind("p3")
                  .to(column)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(
                      ResultSetMetadata.newBuilder()
                          .setRowType(
                              StructType.newBuilder()
                                  .addFields(
                                      StructType.Field.newBuilder()
                                          .setName("SPANNER_TYPE")
                                          .setType(
                                              Type.newBuilder().setCode(TypeCode.STRING).build())
                                          .build())
                                  .build())
                          .build())
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setStringValue("varchar").build())
                          .build())
                  .build()));
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(schema)
                  .bind("p2")
                  .to("Singers")
                  .bind("p3")
                  .to(column)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(
                      ResultSetMetadata.newBuilder()
                          .setRowType(
                              StructType.newBuilder()
                                  .addFields(
                                      StructType.Field.newBuilder()
                                          .setName("SPANNER_TYPE")
                                          .setType(
                                              Type.newBuilder().setCode(TypeCode.STRING).build())
                                          .build())
                                  .build())
                          .build())
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setStringValue("varchar").build())
                          .build())
                  .build()));
    }
    Map<String, String> columnTypes = new HashMap<>();
    columnTypes.put("SingerId", "bigint");
    columnTypes.put("FirstName", "varchar");
    columnTypes.put("LastName", "varchar");

    for (Map.Entry<String, String> entry : columnTypes.entrySet()) {
      String column = entry.getKey();
      String type = entry.getValue();

      params = parser.convertPositionalParametersToNamedParameters('?', GET_SPANNER_TYPE_STATEMENT);

      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to(schema.toLowerCase())
                  .bind("p2")
                  .to("Singers")
                  .bind("p3")
                  .to(column)
                  .build(),
              ResultSet.newBuilder()
                  .setMetadata(
                      ResultSetMetadata.newBuilder()
                          .setRowType(
                              StructType.newBuilder()
                                  .addFields(
                                      StructType.Field.newBuilder()
                                          .setName("SPANNER_TYPE")
                                          .setType(
                                              Type.newBuilder().setCode(TypeCode.STRING).build())
                                          .build())
                                  .build())
                          .build())
                  .addRows(
                      ListValue.newBuilder()
                          .addValues(Value.newBuilder().setStringValue(type).build())
                          .build())
                  .build()));
    }
    sql =
        "select view_definition from information_schema.views where table_name='Singers' and table_schema=? and table_catalog=?";
    params = parser.convertPositionalParametersToNamedParameters('?', sql);
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog)
                .bind("p2")
                .to(schema)
                .build(),
            ResultSet.newBuilder()
                .setMetadata(
                    ResultSetMetadata.newBuilder()
                        .setRowType(
                            StructType.newBuilder()
                                .addFields(
                                    StructType.Field.newBuilder()
                                        .setName("VIEW_DEFINITION")
                                        .setType(
                                            Type.newBuilder().setCode(TypeCode.STRING).build())))
                        .build())
                .addRows(
                    ListValue.newBuilder()
                        .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                        .build())
                .build()));
  }
}
