/**
 * Copyright 2024 Google LLC
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

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AlterSequenceGenerator;
import liquibase.statement.core.AlterSequenceStatement;
import liquibase.statement.core.RenameSequenceStatement;

public class AlterSequenceGeneratorSpanner extends AlterSequenceGenerator {
  static final String ALTER_SEQUENCE_VALIDATION_ERROR =
      "The Liquibase Cloud Spanner extension does not support altering sequences from XML. Please use SQL to create the change instead.";

  @Override
  public ValidationErrors validate(
      AlterSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = super.validate(statement, database, sqlGeneratorChain);
    errors.addError(ALTER_SEQUENCE_VALIDATION_ERROR);
    return errors;
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(AlterSequenceStatement statement, Database database) {
    return (database instanceof ICloudSpanner);
  }
}
