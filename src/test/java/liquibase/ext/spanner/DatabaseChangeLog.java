package liquibase.ext.spanner;

import com.google.cloud.Timestamp;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import com.google.spanner.v1.StructType.Field;

class DatabaseChangeLog {
  String id;
  String author;
  String filename;
  Timestamp dateExecuted;
  long orderExecuted;
  String execType;
  String md5;
  String description;
  String comments;
  String tag;
  String liquibase;
  String contexts;
  String labels;
  String deploymentId;
  
  static final ResultSetMetadata DATABASECHANGELOG_METADATA =
      ResultSetMetadata.newBuilder()
          .setRowType(
              StructType.newBuilder()
                  .addFields(
                      Field.newBuilder()
                          .setName("ID")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("AUTHOR")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("FILENAME")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("DATEEXECUTED")
                          .setType(Type.newBuilder().setCode(TypeCode.TIMESTAMP).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("ORDEREXECUTED")
                          .setType(Type.newBuilder().setCode(TypeCode.INT64).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("EXECTYPE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("MD5SUM")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("DESCRIPTION")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("COMMENTS")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("TAG")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("LIQUIBASE")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("CONTEXTS")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("LABELS")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .addFields(
                      Field.newBuilder()
                          .setName("DEPLOYMENT_ID")
                          .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                          .build())
                  .build())
          .build();

  static ResultSet createChangeSetResultSet(Iterable<DatabaseChangeLog> rows) {
    ResultSet.Builder builder = ResultSet.newBuilder().setMetadata(DATABASECHANGELOG_METADATA);
    for (DatabaseChangeLog row : rows) {
      builder.addRows(ListValue.newBuilder()
          .addValues(createStringOrNullValue(row.id))
          .addValues(createStringOrNullValue(row.author))
          .addValues(createStringOrNullValue(row.filename))
          .addValues(createStringOrNullValue(row.dateExecuted.toString()))
          .addValues(createStringOrNullValue(String.valueOf(row.orderExecuted)))
          .addValues(createStringOrNullValue(row.execType))
          .addValues(createStringOrNullValue(row.md5))
          .addValues(createStringOrNullValue(row.description))
          .addValues(createStringOrNullValue(row.comments))
          .addValues(createStringOrNullValue(row.tag))
          .addValues(createStringOrNullValue(row.liquibase))
          .addValues(createStringOrNullValue(row.contexts))
          .addValues(createStringOrNullValue(row.labels))
          .addValues(createStringOrNullValue(row.deploymentId))
          );
    }
    return builder.build();
  }
  
  static Value createStringOrNullValue(String val) {
    Value.Builder builder = Value.newBuilder();
    if (val == null) {
      builder.setNullValue(NullValue.NULL_VALUE);
    } else {
      builder.setStringValue(val);
    }
    return builder.build();
  }
}
