# Liquibase with Spanner support
It currently builds two artifacts:

* Liquibase Spanner extension ShadowJar bundled with required dependencies
* Docker container with Liquibase itself and all needed dependencies

Liquibase provides a number of features that are useful for trunk-based development,
among which are `labels` and `contexts`. These allow individual changesets to be marked with 
feature names and an expression to be used within the pipeline to determine which are to be
deployed into particular environments:

* https://www.liquibase.org/documentation/labels.html
* https://www.liquibase.org/2014/11/contexts-vs-labels.html

Liquibase also allows for automated rollback of SQL where possible, using markup within the SQL-
formatted changesets.

# Building
This is a standard Gradle project with Jib integration, so can be built and tested with the following commands.
Starting spanner emulator is required for tests
* `./gradlew build`
* `./gradlew test`
* `./gradlew jibDocker` (to create a local docker container)
* `./gradlew shadowJar` (to create a uber-Jar for packaging with Liquibase)

# ShadowJar
The ShadowJar contains the extension, Spanner JDBC Driver, and dependencies. This can be included
in the classpath of Liquibase the CLI for interacting with Spanner databases. See run.sh for an
example of using this.

# Docker container
The docker container contains the contents of the ShadowJar and Liquibase itself. This is a runnable
container that embeds everything you need to run Liquibase. See run.sh for an example of using
this.

# Additional References
* [Use Liquibase to Safely Evolve Your Database Schema](https://www.baeldung.com/liquibase-refactor-schema-of-java-app)
* [Install and configure Cloud Spanner Emulator](https://medium.com/google-cloud/cloud-spanner-emulator-bf12d141c12)
* [Spanner Emulator Docs](https://cloud.google.com/spanner/docs/emulator)
* ["rollback" feature using "SQL formatted output" does not work as expected in Liquibase 3.3.0 (Oracle DB)](https://web.archive.org/web/20170923161755/https://forum.liquibase.org/topic/rollback-feature-using-sql-formatted-output-does-not-work-as-expected-in-liquibase-3-3-0-oracle-db)
* [Spanner Java API example](https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/spanner/cloud-client/src/main/java/com/example/spanner/SpannerSample.java)
* [Flyway Github - Request to add Spanner Support](https://github.com/flyway/flyway/pull/1880)

# Issues
Please feel free to report issues and send pull requests, but note that this application is not officially supported as part of the Cloud Spanner product.

# NOTES:
 * We strongly recommend to use [Liquibase SQL format](https://docs.liquibase.com/concepts/basic/sql-format.html). This allows
   you to explicitly write Spanner SQL. Liquibase SQL generation for Spanner is incomplete. This is required for rollbacks as well.
 * This is *alpha*.

