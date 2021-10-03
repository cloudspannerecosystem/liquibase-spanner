/**
 * Copyright 2020 Google LLC
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package liquibase.ext.spanner.sqlgenerator;

import java.util.ArrayList;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import liquibase.statement.core.InsertOrUpdateStatement;

public class InsertOrUpdateGeneratorSpanner extends InsertOrUpdateGenerator {
  @Override
  public boolean supports(InsertOrUpdateStatement statement, Database database) {
    return database instanceof ICloudSpanner;
  }

  @Override
  public Sql[] generateSql(InsertOrUpdateStatement insertOrUpdateStatement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    // Cloud Spanner does not have an UPSERT / MERGE DML statement, so we will generate both an
    // INSERT and an UPDATE statement. The INSERT statement will check whether the record already
    // exists, and only insert it in that case. The record is always updated.
    //
    // The most efficient way to do this in Cloud Spanner would be to use an InsertOrUpdate
    // mutation. That is however not supported through Liquibase, as it requires the update to use
    // SQL.
    ArrayList<Sql> sqlList = new ArrayList<>(2);
    if (!insertOrUpdateStatement.getOnlyUpdate()) {
      sqlList.add(
          new UnparsedSql(getInsertStatement(insertOrUpdateStatement, database, sqlGeneratorChain),
              "", getAffectedTable(insertOrUpdateStatement)));
    }

    String whereClause = getWhereClause(insertOrUpdateStatement, database);
    try {
      String update =
          getUpdateStatement(insertOrUpdateStatement, database, whereClause, sqlGeneratorChain);
      if (update.endsWith("\n") && update.length() > 1) {
        update = update.substring(0, update.length() - 1);
      }
      if (update.endsWith(";") && update.length() > 1) {
        update = update.substring(0, update.length() - 1);
      }
      sqlList.add(new UnparsedSql(update, "", getAffectedTable(insertOrUpdateStatement)));
    } catch (LiquibaseException e) {
    }
    return sqlList.toArray(new Sql[sqlList.size()]);
  }

  @Override
  protected String getInsertStatement(InsertOrUpdateStatement insertOrUpdateStatement,
      Database database, SqlGeneratorChain sqlGeneratorChain) {
    InsertWithSelectGeneratorSpanner insertGenerator = new InsertWithSelectGeneratorSpanner();
    StringBuffer sql = new StringBuffer(
        insertGenerator.generateSql(insertOrUpdateStatement, database, sqlGeneratorChain)[0]
            .toSql());
    sql
        .append(" FROM UNNEST([1])") // only SELECT statements with a FROM may have a WHERE clause.
        .append(" WHERE NOT EXISTS (") // only insert if the row does not already exist.
        .append("SELECT ")
        .append(insertOrUpdateStatement.getPrimaryKey())
        .append(" FROM ")
        .append(database.escapeTableName(insertOrUpdateStatement.getCatalogName(),
            insertOrUpdateStatement.getSchemaName(), insertOrUpdateStatement.getTableName()))
        .append(" WHERE ")
        .append(getWhereClause(insertOrUpdateStatement, database))
        .append(")");

    return sql.toString();
  }

  @Override
  protected String getRecordCheck(InsertOrUpdateStatement insertOrUpdateStatement,
      Database database, String whereClause) {
    return "";
  }

  @Override
  protected String getElse(Database database) {
    return "";
  }

}
