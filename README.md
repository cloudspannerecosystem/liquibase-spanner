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
This is a standard Gradle project with Jib integration, so can be built and tested with:
* `./gradlew build`
* `./gradlew test`
* `./gradlew integrationTest`
* `./gradlew jibDocker` (to create a local docker container)
* `./gradlew shadowJar` (to create a uber-Jar for packaging with Liquibase)

# Testing
Unit tests and container-based Spanner emulator tests are done as part of the build. It uses
[testcontainers](www.testcontainers.org) which has some [requirements](https://www.testcontainers.org/supported_docker_environment/).

For running tests against a real Spanner, please set the environment variables SPANNER_PROJECT and
SPANNER_INSTANCE to your instance and run `./gradlew integrationTest`.

# ShadowJar
The ShadowJar contains the extension, Spanner JDBC Driver, and dependencies. This can be included
in the classpath of Liquibase the CLI for interacting with Spanner databases. See run.sh for an
example of using this.

# Docker container
The docker container contains the contents of the ShadowJar and Liquibase itself. This is a runnable
container that embeds everything you need to run Liquibase. See run.sh for an example of using
this.

# Examples

See [examples](example/README.md) for a series of changes using Liquibase.

# Supported Features

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:

> createTable, dropTable, addColumn, modifyDataType, addNotNullConstraint, dropColumn, createIndex, dropIndex, addForeignKeyConstraint, dropForeignKeyConstraint, dropAllForeignKeyConstraints, addLookupTable

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are not allowed with Cloud Spanner.

> addAutoIncrement, addDefaultValue, addPrimaryKey, addUniqueConstraint, dropUniqueConstraint, createProcedure, createSequence, createView, dropDefaultValue, dropNotNullConstraint, dropPrimaryKey, dropProcedure, dropSequence, dropView, renameColumn, renameSequence, renameTable, renameView, setColumnRemarks, setTableRemarks

The following data DML are supported:

> delete, insert, loadData, loadUpdateData

NOTE:
 * Instead of addAutoIncrement use allow_commit_timestamp. See examples.
 * Instead of unique constraints use unique indexes.

TODO:
 * mergeColumns
 * alterSequence - is this blocked?
 * delete/insert - do these just work?

# Additional References
* https://www.baeldung.com/liquibase-refactor-schema-of-java-app
* https://forum.liquibase.org/topic/rollback-feature-using-sql-formatted-output-does-not-work-as-expected-in-liquibase-3-3-0-oracle-db
* https://forum.liquibase.org/#topic/49382000000028385
* https://cloud.google.com/spanner/docs/emulator
* https://medium.com/google-cloud/cloud-spanner-emulator-bf12d141c12
* https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/spanner/cloud-client/src/main/java/com/example/spanner/SpannerSample.java
* https://github.com/flyway/flyway/pull/1880

# Issues
Please feel free to report issues and send pull requests, but note that this application is not officially supported as part of the Cloud Spanner product.

# NOTES:

 * This is *alpha*.

