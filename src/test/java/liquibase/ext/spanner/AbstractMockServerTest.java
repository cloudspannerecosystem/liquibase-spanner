package liquibase.ext.spanner;

import static liquibase.ext.spanner.JdbcMetadataQueries.*;
import static org.junit.Assert.fail;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.admin.database.v1.MockDatabaseAdminImpl;
import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractMockServerTest {

  protected static final AbstractStatementParser PARSER_PG =
      AbstractStatementParser.getInstance(Dialect.POSTGRESQL);
  protected static final AbstractStatementParser PARSER =
      AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL);

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
      Statement.of(
          "SELECT MD5SUM FROM DATABASECHANGELOG WHERE MD5SUM IS NOT NULL AND MD5SUM NOT LIKE '9:%'");
  static final Statement SELECT_MAX_ORDER_EXEC =
      Statement.of("SELECT MAX(ORDEREXECUTED) FROM DATABASECHANGELOG");
  static final Statement INSERT_DATABASECHANGELOG =
      Statement.of(
          "INSERT INTO DATABASECHANGELOG (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, EXECTYPE, CONTEXTS, LABELS, LIQUIBASE, DEPLOYMENT_ID)");

  static final String GET_COLUMN_DEFAULT_STATEMENT =
      "SELECT DISTINCT COLUMN_DEFAULT AS COLUMN_DEF FROM INFORMATION_SCHEMA.COLUMNS WHERE LOWER(TABLE_CATALOG) = ? AND LOWER(TABLE_SCHEMA) = ? AND LOWER(TABLE_NAME) = ? AND LOWER(COLUMN_NAME) = ?";

  static final String GET_SPANNER_TYPE_STATEMENT =
      "SELECT SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
          + "WHERE LOWER(TABLE_SCHEMA) = ? "
          + "AND LOWER(TABLE_NAME) = ? "
          + "AND LOWER(COLUMN_NAME) = ?";

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

  static final String DB_ID_GOOGLESQL = "projects/p/instances/i/databases/db";
  static final String DB_ID_POSTGRESQL = "projects/p/instances/i/databases/db_pg";

  protected static MockSpannerServiceImpl mockSpanner;
  protected static MockDatabaseAdminImpl mockAdmin;
  protected static Server server;
  private static InetSocketAddress address;
  protected static AtomicBoolean receivedRequestWithNonLiquibaseToken = new AtomicBoolean();
  private static String invalidClientLibToken;

  @BeforeAll
  static void startStaticServer() throws IOException {
    startStaticServer(null, null);
  }

  static void startStaticServer(@Nullable String liquibaseSchema, @Nullable String defaultSchema)
      throws IOException {
    mockSpanner = new MockSpannerServiceImpl();
    mockSpanner.setAbortProbability(0.0D);
    mockAdmin = new MockDatabaseAdminImpl();
    address = new InetSocketAddress("localhost", 0);
    server =
        NettyServerBuilder.forAddress(address)
            .addService(mockSpanner)
            .addService(mockAdmin)
            // Add a server interceptor that will check that we receive the client lib
            // token that we expect.
            .intercept(
                new ServerInterceptor() {
                  @Override
                  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                      ServerCall<ReqT, RespT> call,
                      Metadata headers,
                      ServerCallHandler<ReqT, RespT> next) {
                    // Ignore create/delete session and execute query calls. Some other
                    // database drivers try to execute a query to check whether the
                    // backend is the 'right' database, instead of at least checking the driver
                    // first...
                    if (!(call.getMethodDescriptor()
                            .getFullMethodName()
                            .equals("google.spanner.v1.Spanner/BatchCreateSessions")
                        || call.getMethodDescriptor()
                            .getFullMethodName()
                            .equals("google.spanner.v1.Spanner/CreateSession")
                        || call.getMethodDescriptor()
                            .getFullMethodName()
                            .equals("google.spanner.v1.Spanner/ExecuteStreamingSql")
                        || call.getMethodDescriptor()
                            .getFullMethodName()
                            .equals("google.spanner.v1.Spanner/DeleteSession")
                        || call.getMethodDescriptor()
                            .getFullMethodName()
                            .equals("google.spanner.v1.Spanner/BeginTransaction"))) {
                      String clientLibToken =
                          headers.get(
                              Key.of("x-goog-api-client", Metadata.ASCII_STRING_MARSHALLER));
                      if (clientLibToken == null || !clientLibToken.contains("sp-liq")) {
                        if (!receivedRequestWithNonLiquibaseToken.getAndSet(true)) {
                          invalidClientLibToken = clientLibToken;
                        }
                      }
                    }
                    return Contexts.interceptCall(Context.current(), call, headers, next);
                  }
                })
            .build()
            .start();

    registerDefaultResults(liquibaseSchema, defaultSchema);
  }

  private static final ResultSetMetadata RESULTSET_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      StructType.Field.newBuilder()
                          .setName("TAB1")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();

  private static final com.google.spanner.v1.ResultSet RESULTSET =
      com.google.spanner.v1.ResultSet.newBuilder()
          .addRows(
              ListValue.newBuilder()
                  .addValues(Value.newBuilder().setStringValue("TAB").build())
                  .build())
          .setMetadata(RESULTSET_METADATA)
          .build();

  static Statement applySchema(Statement stmt, String schema) {
    if (schema == null || schema.isEmpty() || schema.equalsIgnoreCase("public")) {
      return stmt;
    }
    return Statement.of(
        stmt.getSql().replace(" DATABASECHANGELOG", " " + schema + ".DATABASECHANGELOG"));
  }

  private static void registerDefaultResults(
      @Nullable String liquibaseSchema, @Nullable String defaultSchema) {
    Map<String, String> databaseChangeLogColumnTypes =
        ImmutableMap.<String, String>builder()
            .put("ID", "character varying")
            .put("AUTHOR", "character varying")
            .put("FILENAME", "character varying")
            .put("DATEEXECUTED", "timestamptz")
            .put("ORDEREXECUTED", "bigint")
            .put("EXECTYPE", "character varying")
            .put("MD5SUM", "character varying")
            .put("DESCRIPTION", "character varying")
            .put("COMMENTS", "character varying")
            .put("TAG", "character varying")
            .put("LIQUIBASE", "character varying")
            .put("CONTEXTS", "character varying")
            .put("LABELS", "character varying")
            .put("DEPLOYMENT_ID", "character varying")
            .build();

    for (Dialect dialect : Dialect.values()) {
      String catalog = dialect == Dialect.POSTGRESQL ? "DB_PG" : "";
      String schema = defaultSchema;
      if (schema == null) {
        schema = dialect == Dialect.POSTGRESQL ? "PUBLIC" : "";
      }
      if (liquibaseSchema == null) {
        liquibaseSchema = schema;
      }

      AbstractStatementParser parser = dialect == Dialect.POSTGRESQL ? PARSER_PG : PARSER;
      AbstractStatementParser.ParametersInfo params;
      String sql;

      sql =
          dialect == Dialect.POSTGRESQL
              ? readSqlFromFile(GET_TABLES, dialect)
              : parser.removeCommentsAndTrim(readSqlFromFile(GET_TABLES, dialect));
      params = parser.convertPositionalParametersToNamedParameters('?', sql);
      mockSpanner.putStatementResult(
          StatementResult.query(
              Statement.newBuilder(params.sqlWithNamedParameters)
                  .bind("p1")
                  .to("CAT")
                  .bind("p2")
                  .to("SCH")
                  .bind("p3")
                  .to("TAB")
                  .bind("p4")
                  .to("TABLE")
                  .bind("p5")
                  .to("VIEW")
                  .build(),
              RESULTSET));

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
              JdbcMetadataQueries.createGetSchemasResultSet("")));
      for (String schemaName : new String[] {schema, liquibaseSchema}) {
        sql =
            dialect == Dialect.POSTGRESQL
                ? readSqlFromFile(GET_TABLES, dialect)
                : parser.removeCommentsAndTrim(readSqlFromFile(GET_TABLES, dialect));
        params = parser.convertPositionalParametersToNamedParameters('?', sql);

        // Register metadata results for Liquibase tables.
        Statement stmt =
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schemaName.toUpperCase()) // Schema
                .bind("p3")
                .to("DATABASECHANGELOG")
                .bind("p4")
                .to("TABLE") // Liquibase searches for tables
                .bind("p5")
                .to("NON_EXISTENT_TYPE")
                .build();

        mockSpanner.putStatementResult(
            StatementResult.query(
                stmt,
                JdbcMetadataQueries.createGetTablesResultSet(
                    schemaName, ImmutableList.of("DATABASECHANGELOG"))));

        Statement stmtLock =
            Statement.newBuilder(params.sqlWithNamedParameters)
                .bind("p1")
                .to(catalog) // Catalog
                .bind("p2")
                .to(schemaName.toUpperCase()) // Schema
                .bind("p3")
                .to("DATABASECHANGELOGLOCK")
                .bind("p4")
                .to("TABLE") // Liquibase searches for tables
                .bind("p5")
                .to("NON_EXISTENT_TYPE")
                .build();

        mockSpanner.putStatementResult(
            StatementResult.query(
                stmtLock,
                JdbcMetadataQueries.createGetTablesResultSet(
                    schemaName, ImmutableList.of("DATABASECHANGELOGLOCK"))));

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
                    .to(schemaName.toUpperCase()) // Schema
                    .bind("p3")
                    .to("DATABASECHANGELOG")
                    .bind("p4")
                    .to("%") // All column names
                    .build(),
                JdbcMetadataQueries.createGetColumnsResultSet(
                    schemaName,
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

        AbstractStatementParser.ParametersInfo paramsSpannerType =
            parser.convertPositionalParametersToNamedParameters('?', GET_SPANNER_TYPE_STATEMENT);

        AbstractStatementParser.ParametersInfo paramsSpannerDefault =
            parser.convertPositionalParametersToNamedParameters('?', GET_COLUMN_DEFAULT_STATEMENT);

        for (Map.Entry<String, String> entry : databaseChangeLogColumnTypes.entrySet()) {
          String column = entry.getKey();
          String type = entry.getValue();

          if (dialect == Dialect.POSTGRESQL) {
            mockSpanner.putStatementResult(
                StatementResult.query(
                    Statement.newBuilder(paramsSpannerType.sqlWithNamedParameters)
                        .bind("p1")
                        .to(schemaName.toLowerCase())
                        .bind("p2")
                        .to("databasechangelog")
                        .bind("p3")
                        .to(column.toLowerCase())
                        .build(),
                    ResultSet.newBuilder()
                        .setMetadata(
                            ResultSetMetadata.newBuilder()
                                .setRowType(
                                    StructType.newBuilder()
                                        .addFields(
                                            Field.newBuilder()
                                                .setName("SPANNER_TYPE")
                                                .setType(
                                                    Type.newBuilder()
                                                        .setCode(TypeCode.STRING)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .addRows(
                            ListValue.newBuilder()
                                .addValues(Value.newBuilder().setStringValue(type).build())
                                .build())
                        .build()));
          }

          // TODO: Remove when the JDBC driver includes the column default in getColumns
          mockSpanner.putStatementResult(
              StatementResult.query(
                  Statement.newBuilder(paramsSpannerDefault.sqlWithNamedParameters)
                      .bind("p1")
                      .to(catalog.toLowerCase())
                      .bind("p2")
                      .to(schemaName.toLowerCase())
                      .bind("p3")
                      .to("databasechangelog")
                      .bind("p4")
                      .to(column.toLowerCase())
                      .build(),
                  ResultSet.newBuilder()
                      .setMetadata(
                          ResultSetMetadata.newBuilder()
                              .setRowType(
                                  StructType.newBuilder()
                                      .addFields(
                                          Field.newBuilder()
                                              .setName("COLUMN_DEF")
                                              .setType(
                                                  Type.newBuilder()
                                                      .setCode(TypeCode.STRING)
                                                      .build())
                                              .build())
                                      .build())
                              .build())
                      .addRows(
                          ListValue.newBuilder()
                              .addValues(
                                  Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                              .build())
                      .build()));
        }
      }

      // Register results for an empty Liquibase database.
      mockSpanner.putStatementResult(
          StatementResult.query(
              applySchema(SELECT_COUNT_FROM_DATABASECHANGELOG, liquibaseSchema),
              createInt64ResultSet(0L)));
      mockSpanner.putStatementResult(
          StatementResult.query(
              applySchema(SELECT_FROM_DATABASECHANGELOG, liquibaseSchema),
              DatabaseChangeLog.createChangeSetResultSet(ImmutableList.of())));
      mockSpanner.putStatementResult(
          StatementResult.query(
              applySchema(SELECT_COUNT_FROM_DATABASECHANGELOGLOCK, liquibaseSchema),
              createInt64ResultSet(0L)));
      mockSpanner.putStatementResult(
          StatementResult.update(
              applySchema(DELETE_FROM_DATABASECHANGELOGLOCK, liquibaseSchema), 0L));
      mockSpanner.putStatementResult(
          StatementResult.update(applySchema(INSERT_DATABASECHANGELOGLOCK, liquibaseSchema), 1L));
      mockSpanner.putStatementResult(
          StatementResult.query(
              applySchema(SELECT_LOCKED, liquibaseSchema), createLockedResultSet(false)));
      mockSpanner.putPartialStatementResult(
          StatementResult.update(applySchema(ACQUIRE_LOCK, liquibaseSchema), 1L));
      mockSpanner.putStatementResult(
          StatementResult.update(applySchema(RELEASE_LOCK, liquibaseSchema), 1L));
      mockSpanner.putStatementResult(
          StatementResult.query(
              applySchema(SELECT_MD5SUM, liquibaseSchema),
              createMd5SumResultSet(ImmutableList.of())));
      mockSpanner.putStatementResult(
          StatementResult.query(
              applySchema(SELECT_MAX_ORDER_EXEC, liquibaseSchema), createInt64ResultSet(0L)));
      mockSpanner.putPartialStatementResult(
          StatementResult.update(applySchema(INSERT_DATABASECHANGELOG, liquibaseSchema), 1L));
    }
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

  @AfterEach
  void checkForLiquibaseToken() {
    if (receivedRequestWithNonLiquibaseToken.get()) {
      // Clear flag for following tests.
      receivedRequestWithNonLiquibaseToken.set(false);
      fail("Server received request with invalid client lib token: " + invalidClientLibToken);
    }
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

  protected static Liquibase getLiquibase(Connection connection, String changeLogFile)
      throws DatabaseException {
    Liquibase liquibase =
        new Liquibase(
            changeLogFile,
            new ClassLoaderResourceAccessor(),
            DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection)));
    return liquibase;
  }

  protected static Liquibase getLiquibase(String connectionUrl, String changeLogFile)
      throws DatabaseException {
    Liquibase liquibase =
        new Liquibase(
            changeLogFile,
            new ClassLoaderResourceAccessor(),
            DatabaseFactory.getInstance().openDatabase(connectionUrl, null, null, null, null));
    return liquibase;
  }

  protected static void addUpdateDdlStatementsResponse(Dialect dialect, String statement) {
    addUpdateDdlStatementsResponse(dialect, ImmutableList.of(statement));
  }

  static void addUpdateDdlStatementsResponse(Dialect dialect, Iterable<String> statements) {
    String dbId = dialect == Dialect.POSTGRESQL ? DB_ID_POSTGRESQL : DB_ID_GOOGLESQL;
    mockAdmin.addResponse(
        Operation.newBuilder()
            .setDone(true)
            .setMetadata(
                Any.pack(
                    UpdateDatabaseDdlMetadata.newBuilder()
                        .addAllCommitTimestamps(ImmutableList.of(Timestamp.now().toProto()))
                        .setDatabase(dbId)
                        .addAllStatements(statements)
                        .build()))
            .setName(String.format("%s/operations/o", dbId))
            .setResponse(Any.pack(Empty.getDefaultInstance()))
            .build());
  }

  static Iterable<String> getUpdateDdlStatementsList(int index) {
    return ((UpdateDatabaseDdlRequest) mockAdmin.getRequests().get(index)).getStatementsList();
  }

  protected static Connection createConnection(Dialect dialect) throws SQLException {
    mockSpanner.putStatementResult(StatementResult.detectDialectResult(dialect));
    return DriverManager.getConnection(createConnectionUrl(dialect));
  }

  protected static String createConnectionUrl(Dialect dialect) {
    // Register the dialect detection result before creating the connection URL
    return new StringBuilder("jdbc:cloudspanner://localhost:")
        .append(server.getPort())
        .append("/")
        .append(dialect == Dialect.POSTGRESQL ? DB_ID_POSTGRESQL : DB_ID_GOOGLESQL)
        .append(";usePlainText=true;minSessions=0")
        .toString();
  }
}
