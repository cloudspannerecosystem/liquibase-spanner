package liquibase.ext.spanner;

import com.google.cloud.spanner.connection.StatementParser;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.DatabaseMetaData;
import java.util.Scanner;

class JdbcMetadataQueries {
  static final String GET_TABLES =
      convertPositionalParametersToNamedParameters(
          StatementParser.removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetTables.sql")));
  static final String GET_COLUMNS =
      convertPositionalParametersToNamedParameters(
          StatementParser.removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetColumns.sql")));

  static final ResultSetMetadata GET_TABLES_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_CAT")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_SCHEM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_TYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("REMARKS")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TYPE_CAT")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TYPE_SCHEM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TYPE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SELF_REFERENCING_COL_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("REF_GENERATION")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING))))
          .build();

  static ResultSet createGetTablesResultSet(Iterable<String> names) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(GET_TABLES_METADATA);
    for (String name : names) {
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(name))
              .addValues(Value.newBuilder().setStringValue("TABLE"))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)));
    }
    return builder.build();
  }

  static final ResultSetMetadata GET_COLUMNS_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_CAT")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_SCHEM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TABLE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("DATA_TYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TYPE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_SIZE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("BUFFER_LENGTH")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("DECIMAL_DIGITS")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("NUM_PREC_RADIX")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("NULLABLE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("REMARKS")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_DEF")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SQL_DATA_TYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SQL_DATETIME_SUB")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("CHAR_OCTET_LENGTH")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("ORDINAL_POSITION")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("IS_NULLABLE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SCOPE_CATALOG")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SCOPE_SCHEMA")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SCOPE_TABLE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("SOURCE_DATA_TYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("IS_AUTOINCREMENT")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("IS_GENERATEDCOLUMN")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING))))
          .build();

  static class ColumnMetaData {
    final String table;
    final String name;
    final int type; // Should be one of java.sql.Types.
    final String typeName;
    final int size;
    final int nullable; // Should be one of java.sql.DatabaseMetaData.columnNullable

    ColumnMetaData(String table, String name, int type, String typeName, int size, int nullable) {
      this.table = table;
      this.name = name;
      this.type = type;
      this.typeName = typeName;
      this.size = size;
      this.nullable = nullable;
    }
  }

  static ResultSet createGetColumnsResultSet(Iterable<ColumnMetaData> cols) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(GET_COLUMNS_METADATA);
    int position = 1;
    for (ColumnMetaData col : cols) {
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(col.table))
              .addValues(Value.newBuilder().setStringValue(col.name))
              .addValues(Value.newBuilder().setStringValue(String.valueOf(col.type)))
              .addValues(Value.newBuilder().setStringValue(col.typeName))
              .addValues(Value.newBuilder().setStringValue(String.valueOf(col.size)))
              .addValues(Value.newBuilder().setStringValue("0")) // BUFFER_LENGTH
              .addValues(Value.newBuilder().setStringValue("0")) // DECIMAL_DIGITS
              .addValues(Value.newBuilder().setStringValue("0")) // NUM_PREC_RADIX
              .addValues(Value.newBuilder().setStringValue(String.valueOf(col.nullable)))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // REMARKS
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // COLUMN_DEF
              .addValues(Value.newBuilder().setStringValue("0")) // SQL_DATA_TYPE
              .addValues(Value.newBuilder().setStringValue("0")) // SQL_DATETIME_SUB
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // CHAR_OCTET_LENGTH
              .addValues(Value.newBuilder().setStringValue(String.valueOf(position)))
              .addValues(
                  Value.newBuilder()
                      .setStringValue(
                          col.nullable == DatabaseMetaData.columnNoNulls
                              ? "NO"
                              : col.nullable == DatabaseMetaData.columnNullable ? "YES" : ""))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // SCOPE_CATALOG
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // SCOPE_SCHEMA
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // SCOPE_TABLE
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE)) // SOURCE_DATA_TYPE
              .addValues(Value.newBuilder().setStringValue("NO")) // IS_AUTOINCREMENT
              .addValues(Value.newBuilder().setStringValue("NO")) // IS_GENERATEDCOLUMN
          );
      position++;
    }
    return builder.build();
  }

  static String readMetaDataSqlFromFile(String filename) {
    InputStream in = CloudSpannerJdbcConnection.class.getResourceAsStream(filename);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    StringBuilder builder = new StringBuilder();
    try (Scanner scanner = new Scanner(reader)) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        builder.append(line).append("\n");
      }
      scanner.close();
    }
    return builder.toString();
  }

  static String convertPositionalParametersToNamedParameters(String sql) {
    int param = 1;
    int index = 0;
    while (index < sql.length()) {
      if (sql.charAt(index) == '?') {
        sql = sql.substring(0, index) + "@p" + param + sql.substring(index + 1);
        param++;
      }
      index++;
    }
    return sql;
  }
}
