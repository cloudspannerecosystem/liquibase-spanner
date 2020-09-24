package liquibase.ext.spanner;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import liquibase.database.Database;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.CreateTableStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SpannerCreateDatabaseChangeLogLockTableGenerator extends CreateDatabaseChangeLogLockTableGenerator {
    final String createTableSQL = "" +
            "CREATE TABLE DATABASECHANGELOGLOCK\n" +
            "(\n" +
            "    id          int64,\n" +
            "    locked      bool,\n" +
            "    lockgranted timestamp,\n" +
            "    lockedby    string(max),\n" +
            ") primary key (id)";

    @Override
    public Sql[] generateSql(CreateDatabaseChangeLogLockTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[]{ new UnparsedSql(createTableSQL) };
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }
}
