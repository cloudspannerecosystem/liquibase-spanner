package liquibase.ext.spanner

import liquibase.harness.BaseHarnessSuite
import liquibase.harness.change.ChangeObjectTests
import liquibase.harness.snapshot.SnapshotObjectTests
import org.junit.platform.suite.api.SelectClasses

@SelectClasses(ChangeObjectTests.class)
class CloudSpannerBaseHarnessSuiteTest extends BaseHarnessSuite {
}
