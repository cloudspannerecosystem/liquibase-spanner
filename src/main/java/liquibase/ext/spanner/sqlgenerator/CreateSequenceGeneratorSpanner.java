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
package liquibase.ext.spanner.sqlgenerator;

import com.google.cloud.spanner.Dialect;
import java.math.BigInteger;
import java.util.Objects;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.spanner.CloudSpanner;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateSequenceGenerator;
import liquibase.statement.core.CreateSequenceStatement;

public class CreateSequenceGeneratorSpanner extends CreateSequenceGenerator {

  @Override
  public ValidationErrors validate(
      CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(statement, database, sqlGeneratorChain);
    errors.checkDisallowedField(
        "cacheSize", statement.getCacheSize(), database, CloudSpanner.class);
    errors.checkDisallowedField("ordered", statement.getOrdered(), database, CloudSpanner.class);
    errors.checkDisallowedField("cycle", statement.getCycle(), database, CloudSpanner.class);
    errors.checkDisallowedField("dataType", statement.getDataType(), database, CloudSpanner.class);
    // Allow setting incrementBy to 1.
    if (!Objects.equals(BigInteger.ONE, statement.getIncrementBy())) {
      errors.checkDisallowedField(
          "incrementBy", statement.getIncrementBy(), database, CloudSpanner.class);
    }

    return errors;
  }

  @Override
  public Sql[] generateSql(
      CreateSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    Dialect dialect = ((ICloudSpanner) database).getDialect();
    StringBuilder queryStringBuilder = new StringBuilder();
    queryStringBuilder.append("CREATE SEQUENCE ");
    queryStringBuilder.append(
        database.escapeSequenceName(
            statement.getCatalogName(), statement.getSchemaName(), statement.getSequenceName()));
    if (dialect == Dialect.POSTGRESQL) {
      queryStringBuilder.append(" bit_reversed_positive");
      if (statement.getMinValue() != null) {
        queryStringBuilder.append(" SKIP RANGE ").append(statement.getMinValue());
      }
      if (statement.getMaxValue() != null) {
        queryStringBuilder.append(" ").append(statement.getMaxValue());
      }
      if (statement.getStartValue() != null) {
        queryStringBuilder.append(" START COUNTER WITH ").append(statement.getStartValue());
      }
    } else {
      queryStringBuilder.append(" OPTIONS (sequence_kind='bit_reversed_positive'");
      if (statement.getMinValue() != null) {
        queryStringBuilder.append(", skip_range_min = ").append(statement.getMinValue());
      }
      if (statement.getMaxValue() != null) {
        queryStringBuilder.append(", skip_range_max = ").append(statement.getMaxValue());
      }
      if (statement.getStartValue() != null) {
        queryStringBuilder.append(", start_with_counter = ").append(statement.getStartValue());
      }
      queryStringBuilder.append(")");
    }
    return new Sql[] {
      new UnparsedSql(queryStringBuilder.toString(), getAffectedSequence(statement))
    };
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(CreateSequenceStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }
}
