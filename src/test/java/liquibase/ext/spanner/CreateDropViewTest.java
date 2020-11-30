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
public class CreateDropViewTest extends AbstractMockServerTest {

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testCreateViewFromYaml() throws Exception {
    for (String file : new String[] {"create-view.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
        fail("missing expected validation exception");
      } catch (ValidationFailedException e) {
        assertThat(e.getMessage())
            .contains(CreateViewGeneratorSpanner.CREATE_VIEW_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }

  @Test
  void testDropViewFromYaml() throws Exception {
    for (String file : new String[] {"drop-view.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
        fail("missing expected validation exception");
      } catch (ValidationFailedException e) {
        assertThat(e.getMessage())
            .contains(DropViewGeneratorSpanner.DROP_VIEW_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }

  @Test
  void testRenameViewFromYaml() throws Exception {
    for (String file : new String[] {"rename-view.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
        fail("missing expected validation exception");
      } catch (ValidationFailedException e) {
        assertThat(e.getMessage())
            .contains(RenameViewGeneratorSpanner.RENAME_VIEW_VALIDATION_ERROR);
      }
    }
    assertThat(mockAdmin.getRequests()).isEmpty();
  }
}
