package liquibase.ext.spanner.sqlgenerator;

import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.exception.CommandExecutionException;
import liquibase.ext.spanner.AbstractMockServerTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.OutputStreamWriter;
import java.sql.Connection;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Execution(ExecutionMode.SAME_THREAD)
public class AlterSequenceTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testAlterSequenceFromYaml() throws Exception {
    for (String file : new String[]{"alter-sequence.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
        CommandExecutionException exception = assertThrows(CommandExecutionException.class,
            () -> liquibase.update(new Contexts("test"), new OutputStreamWriter(System.out)));
        assertThat(exception.getMessage()).contains(
            AlterSequenceGeneratorSpanner.ALTER_SEQUENCE_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }
}