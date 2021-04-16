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
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableList;
import java.util.Set;
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

@Execution(ExecutionMode.SAME_THREAD)
public class GenerateSnapshotTest extends AbstractMockServerTest {
  
  @BeforeAll
  static void setupResults() {
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
            JdbcMetadataQueries.createGetIndexInfoResultSet(ImmutableList.of())));
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
    }
  }
}
