package liquibase.ext.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
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
import java.sql.DriverManager;
import java.sql.SQLException;
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
  private static final ResultSetMetadata COUNT_METADATA =
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

  protected static final String DB_ID = "projects/p/instances/i/databases/d";
  protected static MockSpannerServiceImpl mockSpanner;
  protected static MockDatabaseAdminImpl mockAdmin;
  private static Server server;
  private static InetSocketAddress address;

  @BeforeAll
  static void startStaticServer() throws IOException {
    mockSpanner = new MockSpannerServiceImpl();
    mockAdmin = new MockDatabaseAdminImpl();
    address = new InetSocketAddress("localhost", 0);
    server =
        NettyServerBuilder.forAddress(address)
            .addService(mockSpanner)
            .addService(mockAdmin)
            .build()
            .start();
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

  static ResultSet createCountResultSet(long count) {
    return com.google.spanner.v1.ResultSet.newBuilder()
        .addRows(
            ListValue.newBuilder()
                .addValues(Value.newBuilder().setStringValue(String.valueOf(count)).build())
                .build())
        .setMetadata(COUNT_METADATA)
        .build();
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
