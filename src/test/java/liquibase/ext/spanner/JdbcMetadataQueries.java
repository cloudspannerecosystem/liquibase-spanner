package liquibase.ext.spanner;

import com.google.cloud.spanner.connection.AbstractStatementParser;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.cloud.spanner.Dialect;
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
          AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL).removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetTables.sql")));
  static final String GET_COLUMNS =
      convertPositionalParametersToNamedParameters(
        AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL).removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetColumns.sql")));
  static final String GET_INDEX_INFO =
      convertPositionalParametersToNamedParameters(
        AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL).removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetIndexInfo.sql")));
  static final String GET_PRIMARY_KEYS =
      convertPositionalParametersToNamedParameters(
        AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL).removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetPrimaryKeys.sql")));
  static final String GET_IMPORTED_KEYS =
      convertPositionalParametersToNamedParameters(
        AbstractStatementParser.getInstance(Dialect.GOOGLE_STANDARD_SQL).removeCommentsAndTrim(
              readMetaDataSqlFromFile("DatabaseMetaData_GetImportedKeys.sql")));

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
  
  static final ResultSetMetadata GET_PRIMARY_KEYS_METADATA =
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
                          .setName("KEY_SEQ")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("PK_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
              )
          .build();
  
  static class PrimaryKeyMetaData {
    final String table;
    final String column;

    PrimaryKeyMetaData(String table, String column) {
      this.table = table;
      this.column = column;
    }
  }

  static ResultSet createGetPrimaryKeysResultSet(Iterable<PrimaryKeyMetaData> keys) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(GET_PRIMARY_KEYS_METADATA);
    int position = 1;
    for (PrimaryKeyMetaData key : keys) {
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(key.table))
              .addValues(Value.newBuilder().setStringValue(key.column))
              .addValues(Value.newBuilder().setStringValue(String.valueOf(position)))
              .addValues(Value.newBuilder().setStringValue("PRIMARY_KEY"))
          );
      position++;
    }
    return builder.build();
  }
  
  static final ResultSetMetadata GET_IMPORTED_KEYS_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("PKTABLE_CAT")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("PKTABLE_SCHEM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("PKTABLE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("PKCOLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("FKTABLE_CAT")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("FKTABLE_SCHEM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("FKTABLE_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("FKCOLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("KEY_SEQ")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("UPDATE_RULE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("DELETE_RULE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("FK_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("PK_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("DEFERRABILITY")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
              )
          .build();
  
  static class ImportedKeyMetaData {
    final String pkTable;
    final String pkColumn;
    final String fkTable;
    final String fkColumn;

    ImportedKeyMetaData(String pkTable, String pkColumn, String fkTable, String fkColumn) {
      this.pkTable = pkTable;
      this.pkColumn = pkColumn;
      this.fkTable = fkTable;
      this.fkColumn = fkColumn;
    }
  }

  static ResultSet createGetImportedKeysResultSet(Iterable<ImportedKeyMetaData> keys) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(GET_IMPORTED_KEYS_METADATA);
    int position = 1;
    for (ImportedKeyMetaData key : keys) {
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(key.pkTable))
              .addValues(Value.newBuilder().setStringValue(key.pkColumn))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(key.fkTable))
              .addValues(Value.newBuilder().setStringValue(key.fkColumn))
              .addValues(Value.newBuilder().setStringValue(String.valueOf(position)))
              .addValues(Value.newBuilder().setStringValue("1"))
              .addValues(Value.newBuilder().setStringValue("0"))
              .addValues(Value.newBuilder().setNullValue(NullValue.NULL_VALUE))
              .addValues(Value.newBuilder().setStringValue("PRIMARY_KEY"))
              .addValues(Value.newBuilder().setStringValue("7"))
          );
      position++;
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
  
  static final ResultSetMetadata GET_INDEX_INFO_METADATA =
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
                          .setName("NON_UNIQUE")
                          .setType(Type.newBuilder().setCode(TypeCode.BOOL)))
                  .addFields(
                      Field.newBuilder()
                          .setName("INDEX_QUALIFIER")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("INDEX_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("TYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("ORDINAL_POSITION")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("COLUMN_NAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("ASC_OR_DESC")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING)))
                  .addFields(
                      Field.newBuilder()
                          .setName("CARDINALITY")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64)))
                  .addFields(
                      Field.newBuilder()
                          .setName("PAGES")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64))))
          .build();
  
  static class IndexMetaData {
    final String table;
    final boolean unique;
    final String name;
    final boolean primaryKey;
    final int ordinalPosition;
    final String column;
    final boolean ascending;

    IndexMetaData(
        String table,
        boolean unique,
        String name,
        boolean primaryKey,
        int ordinalPosition,
        String column,
        boolean ascending) {
      this.table = table;
      this.unique = unique;
      this.name = name;
      this.primaryKey = primaryKey;
      this.ordinalPosition = ordinalPosition;
      this.column = column;
      this.ascending = ascending;
    }
  }

  static ResultSet createGetIndexInfoResultSet(Iterable<IndexMetaData> indexes) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(GET_INDEX_INFO_METADATA);
    for (IndexMetaData index : indexes) {
      builder.addRows(
          ListValue.newBuilder()
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(index.table))
              .addValues(Value.newBuilder().setBoolValue(!index.unique))
              .addValues(Value.newBuilder().setStringValue(""))
              .addValues(Value.newBuilder().setStringValue(index.name))
              .addValues(Value.newBuilder().setStringValue(index.primaryKey ? "1" : "2"))
              .addValues(Value.newBuilder().setStringValue(String.valueOf(index.ordinalPosition)))
              .addValues(Value.newBuilder().setStringValue(index.column))
              .addValues(Value.newBuilder().setStringValue(index.ascending ? "A" : "D"))
              .addValues(Value.newBuilder().setStringValue("-1"))
              .addValues(Value.newBuilder().setStringValue("-1"))
          );
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
