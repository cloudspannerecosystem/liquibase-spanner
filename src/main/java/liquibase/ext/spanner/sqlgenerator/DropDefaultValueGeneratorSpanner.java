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

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropDefaultValueGenerator;
import liquibase.statement.core.DropDefaultValueStatement;

public class DropDefaultValueGeneratorSpanner extends DropDefaultValueGenerator {
  static final String DROP_DEFAULT_VALUE_VALIDATION_ERROR =
      "Cloud Spanner does not support dropping a default value from a column";

  @Override
  public ValidationErrors validate(
      DropDefaultValueStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(statement, database, sqlGeneratorChain);
    errors.addError(DROP_DEFAULT_VALUE_VALIDATION_ERROR);
    return errors;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(DropDefaultValueStatement statement, Database database) {
    return (database instanceof CloudSpanner);
  }
}
