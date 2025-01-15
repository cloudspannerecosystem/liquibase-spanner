/**
 * Copyright 2021 Google LLC
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
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.core.AddColumnGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.AddColumnStatement;

import java.sql.Timestamp;

public class AddColumnGeneratorSpanner extends AddColumnGenerator {

    @Override
    public int getPriority() {
        return SqlGenerator.PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return (database instanceof ICloudSpanner);
    }

    @Override
    protected String generateSingleColumnSQL(AddColumnStatement statement, Database database) {
        if (!(database instanceof ICloudSpanner)) {
            return super.generateSingleColumnSQL(statement, database);
        }

        DatabaseDataType columnType = null;

        if (statement.getColumnType() != null) {
            columnType = DataTypeFactory.getInstance().fromDescription(statement.getColumnType() + (statement.isAutoIncrement() ? "{autoIncrement:true}" : ""), database).toDatabaseDataType(database);
        }

        // Add "COLUMN" keyword before column name for compatibility with Cloud Spanner
        String alterTable = " ADD COLUMN " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName());

        if (columnType != null) {
            alterTable += " " + columnType;
        }

        if (!statement.isNullable()) {
            alterTable += " NOT NULL";
        }

        // Wrap default value in parentheses
        Object defaultValue = statement.getDefaultValue();
        if (defaultValue != null) {
            String wrappedDefaultValue;
            if (defaultValue instanceof DatabaseFunction){
                wrappedDefaultValue = "(" + defaultValue + ")";
            }else if (defaultValue instanceof Boolean){
                if (defaultValue == Boolean.TRUE){
                    wrappedDefaultValue = "(TRUE)";
                }else {
                    wrappedDefaultValue = "(FALSE)";
                }
            }else if (defaultValue instanceof Number){
                wrappedDefaultValue = "(" + defaultValue + ")";
            }else if (defaultValue instanceof Timestamp){
                String clause =  DataTypeFactory.getInstance().fromDescription(statement.getColumnType(), database).objectToSql(defaultValue, database);
                wrappedDefaultValue = "(" + clause + ")";
            }
            else{
                wrappedDefaultValue = "('" + defaultValue + "')";
            }
            alterTable += " DEFAULT " + wrappedDefaultValue;
        }
        return alterTable;
    }

}
