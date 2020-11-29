package liquibase.ext.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.admin.database.v1.MockDatabaseAdminImpl;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.common.collect.ImmutableList;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractMockServerTest {
  static final Statement SELECT_FROM_DATABASECHANGELOG =
      Statement.of("SELECT * FROM DATABASECHANGELOG ORDER BY DATEEXECUTED ASC, ORDEREXECUTED ASC");
  static final Statement SELECT_COUNT_FROM_DATABASECHANGELOG =
      Statement.of("SELECT COUNT(*) FROM DATABASECHANGELOG");
  static final Statement SELECT_COUNT_FROM_DATABASECHANGELOGLOCK =
      Statement.of("SELECT COUNT(*) FROM DATABASECHANGELOGLOCK");
  static final Statement DELETE_FROM_DATABASECHANGELOGLOCK =
      Statement.of("DELETE FROM DATABASECHANGELOGLOCK WHERE true");
  static final Statement INSERT_DATABASECHANGELOGLOCK =
      Statement.of("INSERT INTO DATABASECHANGELOGLOCK (ID, LOCKED) VALUES (1, FALSE)");
  static final Statement SELECT_LOCKED =
      Statement.of("SELECT LOCKED FROM DATABASECHANGELOGLOCK WHERE ID=1");
  static final Statement ACQUIRE_LOCK =
      Statement.of("UPDATE DATABASECHANGELOGLOCK SET LOCKED = TRUE, LOCKEDBY = ");
  static final Statement RELEASE_LOCK =
      Statement.of(
          "UPDATE DATABASECHANGELOGLOCK SET LOCKED = FALSE, LOCKEDBY = NULL, LOCKGRANTED = NULL WHERE ID = 1");
  static final Statement SELECT_MD5SUM =
      Statement.of("SELECT MD5SUM FROM DATABASECHANGELOG WHERE MD5SUM IS NOT NULL");
  static final Statement SELECT_MAX_ORDER_EXEC =
      Statement.of("SELECT MAX(ORDEREXECUTED) FROM DATABASECHANGELOG");
  static final Statement INSERT_DATABASECHANGELOG =
      Statement.of(
          "INSERT INTO DATABASECHANGELOG (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID)");

  private static final ResultSetMetadata SINGLE_COL_INT64_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("C")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .build())
          .build();
  private static final ResultSetMetadata LOCKED_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("LOCKED")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL).build())
                          .build())
                  .build())
          .build();
  private static final ResultSetMetadata MD5SUM_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("MD5SUM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();

  protected static final String DB_ID = "projects/p/instances/i/databases/d";
  protected static MockSpannerServiceImpl mockSpanner;
  protected static MockDatabaseAdminImpl mockAdmin;
  private static Server server;
  private static InetSocketAddress address;

  @BeforeAll
  static void startStaticServer() throws IOException {
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0D);
    mockAdmin = new MockDatabaseAdminImpl();
    address = new InetSocketAddress("localhost", 0);
    server =
        NettyServerBuilder.forAddress(address)
            .addService(mockSpanner)
            .addService(mockAdmin)
            .build()
            .start();

    registerDefaultResults();
  }

  private static void registerDefaultResults() {
    // Register metadata results for Liquibase tables.
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_TABLES)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("DATABASECHANGELOG")
                .bind("p4")
                .to("TABLE") // Liquibase searches for tables
                .bind("p5")
                .to("NON_EXISTENT_TYPE") // This is a trick in the JDBC driver to simplify the query
                .build(),
            JdbcMetadataQueries.createGetTablesResultSet(ImmutableList.of("DATABASECHANGELOG"))));
    mockSpanner.putStatementResult(
        StatementResult.query(
            Statement.newBuilder(JdbcMetadataQueries.GET_COLUMNS)
                .bind("p1")
                .to("") // Catalog
                .bind("p2")
                .to("") // Schema
                .bind("p3")
                .to("DATABASECHANGELOG")
                .bind("p4")
                .to("%") // All column names
                .build(),
            JdbcMetadataQueries.createGetColumnsResultSet(
                ImmutableList.of(
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "ID",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "AUTHOR",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "FILENAME",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "DATEEXECUTED",
                        Types.TIMESTAMP,
                        "TIMESTAMP",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "ORDEREXECUTED",
                        Types.BIGINT,
                        "INT64",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "EXECTYPE",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "MD5SUM",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "DESCRIPTION",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "COMMENTS",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "TAG",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "LIQUIBASE",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "CONTEXTS",
                        Types.NVARCHAR,
                        "STRING",
                        255,
                        DatabaseMetaData.columnNullable),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "LABELS",
                        Types.NVARCHAR,
                        "STRING",
                        255,
                        DatabaseMetaData.columnNullable),
                    new JdbcMetadataQueries.ColumnMetaData(
                        "DATABASECHANGELOG",
                        "DEPLOYMENT_ID",
                        Types.NVARCHAR,
                        "STRING",
                        0,
                        DatabaseMetaData.columnNoNulls)))));

    // Register results for an empty Liquibase database.
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_COUNT_FROM_DATABASECHANGELOG, createInt64ResultSet(0L)));
    mockSpanner.putStatementResult(
        StatementResult.query(
            SELECT_FROM_DATABASECHANGELOG,
            DatabaseChangeLog.createChangeSetResultSet(ImmutableList.of())));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_COUNT_FROM_DATABASECHANGELOGLOCK, createInt64ResultSet(0L)));
    mockSpanner.putStatementResult(StatementResult.update(DELETE_FROM_DATABASECHANGELOGLOCK, 0L));
    mockSpanner.putStatementResult(StatementResult.update(INSERT_DATABASECHANGELOGLOCK, 1L));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_LOCKED, createLockedResultSet(false)));
    mockSpanner.putPartialStatementResult(StatementResult.update(ACQUIRE_LOCK, 1L));
    mockSpanner.putStatementResult(StatementResult.update(RELEASE_LOCK, 1L));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_MD5SUM, createMd5SumResultSet(ImmutableList.of())));
    mockSpanner.putStatementResult(
        StatementResult.query(SELECT_MAX_ORDER_EXEC, createInt64ResultSet(0L)));
    mockSpanner.putPartialStatementResult(StatementResult.update(INSERT_DATABASECHANGELOG, 1L));
  }

  @AfterAll
  static void stopServer() throws Exception {
    // Make sure to close all connections before we stop the mock server.
    try {
      ConnectionOptions.closeSpanner();
    } catch (SpannerException e) {
      // ignore
    }
    server.shutdown();
    server.awaitTermination();
  }

  static ResultSet createInt64ResultSet(long value) {
    return com.google.spanner.v1.ResultSet.newBuilder()
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue(String.valueOf(value)).build())
                .build())
        .setMetadata(SINGLE_COL_INT64_METADATA)
        .build();
  }

  static ResultSet createLockedResultSet(boolean locked) {
    return com.google.spanner.v1.ResultSet.newBuilder()
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setBoolValue(locked).build())
                .build())
        .setMetadata(LOCKED_METADATA)
        .build();
  }

  static ResultSet createMd5SumResultSet(Iterable<String> sums) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(MD5SUM_METADATA);
    for (String sum : sums) {
      builder.addRows(
          ListValue.newBuilder().addValues(Value.newBuilder().setStringValue(sum).build()).build());
    }
    return builder.build();
  }

  static Liquibase getLiquibase(Connection connection, String changeLogFile)
      throws DatabaseException {
    Liquibase liquibase =
        new Liquibase(
            changeLogFile,
            new ClassLoaderResourceAccessor(),
            DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection)));
    return liquibase;
  }

  static void addUpdateDdlStatementsResponse(String statement) {
    addUpdateDdlStatementsResponse(ImmutableList.of(statement));
  }

  static void addUpdateDdlStatementsResponse(Iterable<String> statements) {
    mockAdmin.addResponse(
        Operation.newBuilder()
            .setDone(true)
            .setMetadata(
                Any.pack(
                    UpdateDatabaseDdlMetadata.newBuilder()
                        .addAllCommitTimestamps(ImmutableList.of(Timestamp.now().toProto()))
                        .setDatabase(DB_ID)
                        .addAllStatements(statements)
                        .build()))
            .setName(String.format("%s/operations/o", DB_ID))
            .setResponse(Any.pack(Empty.getDefaultInstance()))
            .build());
  }

  static Iterable<String> getUpdateDdlStatementsList(int index) {
    return ((UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(index)).getStatementsList();
  }

  static Connection createConnection() throws SQLException {
    StringBuilder url =
        new StringBuilder("jdbc:cloudspanner://localhost:")
            .append(server.getPort())
            .append("/projects/p/instances/i/databases/d;usePlainText=true");
    return DriverManager.getConnection(url.toString());
  }
}
