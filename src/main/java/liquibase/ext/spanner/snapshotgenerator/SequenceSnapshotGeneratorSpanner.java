package liquibase.ext.spanner.snapshotgenerator;

import liquibase.database.Database;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.SequenceSnapshotGenerator;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Schema;

import java.util.ArrayList;
import java.util.List;

public class SequenceSnapshotGeneratorSpanner extends SequenceSnapshotGenerator {

  /**
   * This generator will be in all chains relating to CloudSpanner, whether or not
   * the objectType is {@link liquibase.structure.core.Sequence}.
   */
  @Override
  public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    if (database instanceof ICloudSpanner) {
      return PRIORITY_DATABASE;
    }
    return PRIORITY_NONE;
  }

  @Override
  protected SqlStatement getSelectSequenceStatement(Schema schema, Database database) {
    if (database instanceof ICloudSpanner) {
      List<String> parameter = new ArrayList<>(2);
      parameter.add(database.getDefaultCatalogName());
      parameter.add(database.getDefaultSchemaName());

      StringBuilder sql = new StringBuilder("SELECT seq.NAME AS SEQUENCE_NAME, ")
                              .append("seq_kind.OPTION_VALUE AS SEQUENCE_KIND, ")
                              .append("skip_max.OPTION_VALUE AS SKIP_RANGE_MAX, ")
                              .append("skip_min.OPTION_VALUE AS SKIP_RANGE_MIN, ")
                              .append("start_counter.OPTION_VALUE AS START_VALUE ")
                              .append("FROM (SELECT DISTINCT NAME ")
                              .append("FROM INFORMATION_SCHEMA.sequence_options ")
                              .append("WHERE CATALOG = ? AND SCHEMA = ?) AS seq ")
                              .append("LEFT JOIN INFORMATION_SCHEMA.sequence_options AS seq_kind ")
                              .append("ON seq.NAME = seq_kind.NAME ")
                              .append("AND seq_kind.OPTION_NAME = 'sequence_kind' ")
                              .append("LEFT JOIN INFORMATION_SCHEMA.sequence_options AS skip_max ")
                              .append("ON seq.NAME = skip_max.NAME ")
                              .append("AND skip_max.OPTION_NAME = 'skip_range_max' ")
                              .append("LEFT JOIN INFORMATION_SCHEMA.sequence_options AS skip_min ")
                              .append("ON seq.NAME = skip_min.NAME ")
                              .append("AND skip_min.OPTION_NAME = 'skip_range_min' ")
                              .append("LEFT JOIN INFORMATION_SCHEMA.sequence_options AS start_counter ")
                              .append("ON seq.NAME = start_counter.NAME ")
                              .append("AND start_counter.OPTION_NAME = 'start_with_counter';");


      return new RawParameterizedSqlStatement(sql.toString(), parameter.toArray());
    }
    return super.getSelectSequenceStatement(schema, database);
  }

  /*
   * If there is a SequenceSnapshotGenerator in the chain, we replace it. Otherwise
   * the chain will execute like normal.
   */
  @Override
  public Class<? extends SnapshotGenerator>[] replaces() {
    return new Class[]{SequenceSnapshotGenerator.class};
  }

  ;
}
