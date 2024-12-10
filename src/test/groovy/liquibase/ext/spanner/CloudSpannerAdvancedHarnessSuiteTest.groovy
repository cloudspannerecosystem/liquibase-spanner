package liquibase.ext.spanner

import liquibase.harness.AdvancedHarnessSuite
import liquibase.harness.snapshot.SnapshotObjectTests
import org.junit.platform.suite.api.SelectClasses

@SelectClasses(SnapshotObjectTests.class)
class CloudSpannerAdvancedHarnessSuiteTest extends AdvancedHarnessSuite{
}
