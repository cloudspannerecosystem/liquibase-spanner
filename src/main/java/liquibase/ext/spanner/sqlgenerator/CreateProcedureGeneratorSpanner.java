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
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateProcedureGenerator;
import liquibase.statement.core.CreateProcedureStatement;

public class CreateProcedureGeneratorSpanner extends CreateProcedureGenerator {
  static final String CREATE_PROCEDURE_VALIDATION_ERROR =
      "Cloud Spanner does not support creating procedures";

  @Override
  public ValidationErrors validate(
      CreateProcedureStatement statement,
      Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors errors = new ValidationErrors();
    errors.addError(CREATE_PROCEDURE_VALIDATION_ERROR);
    return errors;
  }
  
  public Sql[] generateSql(CreateProcedureStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    // The normal way of failing the generation by adding a validation error does not work for CREATE PROCEDURE.
    // This seems to be caused by the fact that CreateProcedureChange is a DbmsTargetedChange.
    throw new UnexpectedLiquibaseException(CREATE_PROCEDURE_VALIDATION_ERROR);
  }

  @Override
  public int getPriority() {
    return SqlGenerator.PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(CreateProcedureStatement statement, Database database) {
    return (database instanceof CloudSpanner);
  }
}
