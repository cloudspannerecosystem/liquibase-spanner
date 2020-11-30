package liquibase.ext.spanner;

import com.google.common.base.MoreObjects;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.DropAllForeignKeyConstraintsChange;
import liquibase.change.core.DropForeignKeyConstraintChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.statement.SqlStatement;

@DatabaseChange(name="dropAllForeignKeyConstraints", description = "Drops all foreign key constraints for a table", priority = ChangeMetaData.PRIORITY_DATABASE, appliesTo = "table")
public class DropAllForeignKeyConstraintsChangeSpanner extends DropAllForeignKeyConstraintsChange {

  @Override
  public boolean supports(Database database) {
    return (database instanceof CloudSpanner);
  }
  
  public SqlStatement[] generateStatements(Database database) {
    List<SqlStatement> sqlStatements = new ArrayList<>();
    
    JdbcConnection connection = (JdbcConnection) database.getConnection();
    try (PreparedStatement ps = connection.prepareStatement("SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_CATALOG=? AND TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_TYPE='FOREIGN KEY'")) {
      ps.setString(1, MoreObjects.firstNonNull(getBaseTableCatalogName(), ""));
      ps.setString(2, MoreObjects.firstNonNull(getBaseTableSchemaName(), ""));
      ps.setString(3, getBaseTableName());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          DropForeignKeyConstraintChange drop = new DropForeignKeyConstraintChange();
          drop.setBaseTableCatalogName(getBaseTableCatalogName());
          drop.setBaseTableSchemaName(getBaseTableSchemaName());
          drop.setBaseTableName(getBaseTableName());
          drop.setConstraintName(rs.getString(1));
          sqlStatements.addAll(Arrays.asList(drop.generateStatements(database)));
        }
      }
    } catch (SQLException | DatabaseException e) {
      throw new UnexpectedLiquibaseException(String.format("Could not retrieve foreign keys for table %s", getBaseTableName()), e);
    }
    
    return sqlStatements.toArray(new SqlStatement[sqlStatements.size()]);
  }

}
