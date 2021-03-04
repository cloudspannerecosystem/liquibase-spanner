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
import liquibase.ext.spanner.ICloudSpanner;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.core.AddColumnGenerator;
import liquibase.statement.core.AddColumnStatement;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

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
        // Create a proxy that will add the COLUMN keyword in front of the column name when it is
        // needed specifically for this change. That prevents the Cloud Spanner emulator from
        // rejecting the DDL statement, as it requires the COLUMN keyword to be included in the
        // statement.
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals("escapeColumnName")) {
                return "COLUMN " + method.invoke(database, args);
            }
            return method.invoke(database, args);
        };
        ICloudSpanner databaseWithColumnKeyword = (ICloudSpanner) Proxy.newProxyInstance(ICloudSpanner.class.getClassLoader(),
                new Class[] { ICloudSpanner.class },
                handler);
        return super.generateSingleColumnSQL(statement, databaseWithColumnKeyword);
    }
}
