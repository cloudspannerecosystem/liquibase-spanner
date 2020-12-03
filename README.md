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
* `./gradle jibDocker` (to create a local docker container)
* `./gradle shadowJar` (to create a uber-Jar for packaging with Liquibase)

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

# Supported Features

| Supported by Spanner             | Remark                                 |
| -------------------------------- | -------------------------------------- |
| createTable                      | Interleaved tales not supported.       |
| dropTable                        |                                        |
| addColumn                        |                                        |
| modifyDataType                   |                                        |
| addNotNullConstraint             |                                        |
| dropColumn                       |                                        |
| createIndex                      |                                        |
| dropIndex                        |                                        |
| addForeignKeyConstraint          |                                        |
| dropForeignKeyConstraint         |                                        |
| dropAllForeignKeyConstraints     |                                        |
| addLookupTable                   |                                        |

| Changes not supported by Spanner | Remark                              |
| -------------------------------- | ----------------------------------- |
| addAutoIncrement                 | Use spanner allow_commit_timestamp? |
| addDefaultValue                  |                                     |
| addPrimaryKey                    |                                     |
| addUniqueConstraint              | Use unique indexes instead.         |
| createProcedure                  |                                     |
| createSequence                   |                                     |
| createView                       |                                     |
| dropDefaultValue                 |                                     |
| dropNotNullConstraint            | Use unique indexes instead.         |
| dropPrimaryKey                   |                                     |
| dropProcedure                    |                                     |
| dropSequence                     |                                     |
| dropUniqueConstraint             |                                     |
| dropView                         |                                     |
| renameColumn                     |                                     |
| renameSequence                   |                                     |
| renameTable                      |                                     |
| renameView                       |                                     |
| setColumnRemarks                 |                                     |
| setTableRemarks                  |                                     |


| Test?                            | Remark  |
| -------------------------------- |:-------:|
| alterSequence                    | To Stop |
| delete                           | To Test |
| insert                           | To Test |
| loadData                         | To Test |
| loadUpdateData                   | To Test |
| mergeColumns                     | To Test |
| update                           | To Test |
| sql                              | N/A     |
| sqlFile                          | N/A     |
| customChange                     | N/A     |
| executeCommand                   | N/A     |
| output                           | N/A     |
| stop                             | N/A     |
| tagDatabase                      | N/A     |



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

 * We strongly recommend to use [Liquibase SQL format](https://docs.liquibase.com/concepts/basic/sql-format.html). This allows
   you to explicitly write Spanner SQL. Liquibase SQL generation for Spanner is incomplete. This is required for rollbacks as well.
 * You need to initialise Liquibase change log tables manually. The SQL used is in [sql/test/resources/initial.spanner.sql](sql/test/resources/initial.spanner.sql).
 * This is *alpha*.

