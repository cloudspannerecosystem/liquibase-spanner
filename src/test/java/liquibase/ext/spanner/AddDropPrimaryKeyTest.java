package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.exception.ValidationFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
public class AddDropPrimaryKeyTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testAddPrimaryKeySingersFromYaml() throws Exception {
    for (String file : new String[] {"add-primary-key-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
        fail("missing expected validation exception");
      } catch (ValidationFailedException e) {
        assertThat(e.getMessage()).contains(SpannerAddPrimaryKeyGenerator.ADD_PK_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }

  @Test
  void testDropPrimaryKeySingersFromYaml() throws Exception {
    for (String file : new String[] {"drop-primary-key-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
        fail("missing expected validation exception");
      } catch (ValidationFailedException e) {
        assertThat(e.getMessage())
            .contains(SpannerDropPrimaryKeyGenerator.DROP_PK_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }
}
