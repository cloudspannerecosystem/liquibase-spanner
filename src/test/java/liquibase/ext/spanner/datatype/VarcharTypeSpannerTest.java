package liquibase.ext.spanner.datatype;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import liquibase.database.core.PostgresDatabase;
import liquibase.ext.spanner.CloudSpanner;
import org.junit.jupiter.api.Test;

public class VarcharTypeSpannerTest {
  private final VarcharTypeSpanner type = new VarcharTypeSpanner();

  @Test
  public void testSupportsDatabase() {
    assertThat(type.supports(mock(PostgresDatabase.class))).isFalse();
    assertThat(type.supports(mock(CloudSpanner.class))).isTrue();
  }
}
