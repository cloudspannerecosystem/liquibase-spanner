/**
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package liquibase.ext.spanner;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.servicelocator.ServiceLocator;

public class CloudSpanner extends AbstractJdbcDatabase {

    // Service Locator is used by Liquibase to find available extension classes.
    // This is needed to register this package to be searched.
    static {

        // Add classpath for extensions
        ServiceLocator.getInstance().addPackageToScan("liquibase.ext.spanner");

        // Register SQL Generators
        SqlGeneratorFactory.getInstance().register(new SpannerCreateTableGenerator());
    }

    public CloudSpanner() {
        SqlGeneratorFactory sqlGeneratorFactory = SqlGeneratorFactory.getInstance();
        sqlGeneratorFactory.register(new CreateDatabaseChangeLogTableGenerator());
        sqlGeneratorFactory.register(new SpannerInitializeChangeLogLockTableGenerator());
        sqlGeneratorFactory.register(new SpannerCreateDatabaseChangeLogTableGenerator());
        sqlGeneratorFactory.register(new UnlockDatabaseChangeLogGenerator());
        sqlGeneratorFactory.register(new LockDatabaseChangeLogGenerator());
    }

    @Override
    public java.lang.Integer getDefaultPort() {
        return Integer.valueOf(9010);
    }

    @Override
    public String getCurrentDateTimeFunction() {
        return "CURRENT_TIMESTAMP()";
    }

    @Override
    public String getShortName() {
        return "cloudspanner";
    }

    @Override
    public String getDefaultDatabaseProductName() {
        return "cloudspanner";
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    public String getDefaultDriver(String url) {
        return "com.google.cloud.spanner.jdbc.JdbcDriver";
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public int getPriority() {
        return 2;
    }

    @Override
    public boolean requiresUsername() {
        return false;
    }

    @Override
    public boolean requiresPassword() {
        return false;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) {
        return conn.getURL().contains("cloudspanner");
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return false;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return false;
    }

    @Override
    public boolean supportsRestrictForeignKeys() {
        return false;
    }

    @Override
    public boolean canCreateChangeLogTable() {
        return false;
    }

    @Override
    public boolean supportsDDLInTransaction() {
        return false;
    }

    @Override
    public boolean supportsPrimaryKeyNames() {
        return false;
    }
}
