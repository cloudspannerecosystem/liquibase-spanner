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
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddUniqueConstraintGenerator;
import liquibase.statement.core.AddUniqueConstraintStatement;

/**
 * Cloud Spanner does not support unique constraints. Applications should create a unique index
 * instead.
 */
public class AddUniqueConstraintGeneratorSpanner extends AddUniqueConstraintGenerator {
  static final String ADD_UNIQUE_CONSTRAINT_VALIDATION_ERROR =
      "Cloud Spanner does not support unique constraints. Use a unique index instead.";

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(AddUniqueConstraintStatement statement, Database database) {
    return (database instanceof CloudSpanner);
  }

  @Override
  public ValidationErrors validate(
      AddUniqueConstraintStatement addUniqueConstraintStatement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors =
        super.validate(addUniqueConstraintStatement, database, sqlGeneratorChain);
    errors.addError(ADD_UNIQUE_CONSTRAINT_VALIDATION_ERROR);
    return errors;
  }
}
