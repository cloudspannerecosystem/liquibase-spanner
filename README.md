
# Liquibase Spanner Extension

A Liquibase extension adding support for Google Cloud Spanner. Include this in your
application project to run Liquibase database migration scripts against a Google
Cloud Spanner database.

## Performance Recommendations
Executing multiple small DDL statements on Cloud Spanner can take a very long time. This means that
the standard recommendation to use as small changesets as possible with Liquibase is not always the
best choice when working with Cloud Spanner. Instead, it is recommended to create changesets that
group multiple DDL statements into one DDL batch. Use [SQL change](https://docs.liquibase.com/change-types/community/sql.html)
and batch the DDL using [batch statements](https://cloud.google.com/spanner/docs/jdbc-session-mgmt-commands#batch_statements).

You can also create a single change set that contains multiple Liquibase changes (e.g. `createTable`)
and create these in a batch by adding a SQL command before and after the changes. See
[create-multiple-tables.spanner.yaml](src/test/resources/create-multiple-tables.spanner.yaml) for
an example.

## Release Notes

#### 4.17.1
* Requires Liquibase 4.17.0
* Updated Google Cloud dependencies to latest version (26.9.0)
* Fixed #159: NullPointerException caused by index

#### 4.17.0
* Requires Liquibase 4.17.0

#### 4.16.1
* Requires Liquibase 4.16.1

#### 4.16.0
* Requires Liquibase 4.16.0

#### 4.15.0
* Requires Liquibase 4.15.0

#### 4.14.0
* Requires Liquibase 4.14.0

#### 4.13.0
* Requires Liquibase 4.13.0

#### 4.12.0
* Requires Liquibase 4.12.0

#### 4.11.0
* Requires Liquibase 4.11.0

#### 4.10.1
* #121 Added support for configured databasechangelog/lock table names. by @cbuschka in #122
* Requires Liquibase 4.10.0

#### 4.10.0
* Requires Liquibase 4.10.0

#### 4.9.1
* Requires Liquibase 4.9.1

#### 4.9.0
* Requires Liquibase 4.9.0

#### 4.8.0
* Requires Liquibase 4.8.0

#### 4.7.1
* Requires Liquibase 4.7.1

#### 4.7.0
* Requires Liquibase 4.7.0

#### 4.6.2
* Requires Liquibase 4.6.2

#### 4.6.1
* Requires Liquibase 4.6.1
* Fixes a bug where statements could be generated in the Spanner dialect when multiple different databases had been configured, and
  the Spanner library was included in the build. See also [#102](https://github.com/cloudspannerecosystem/liquibase-spanner/pull/102)

#### 4.5.0
* Requires Liquibase 4.5.0
* Adds support for `CREATE [OR REPLACE] VIEW` and `DROP VIEW` [statements](https://cloud.google.com/spanner/docs/data-definition-language#view_statements)

#### 4.4.3
* Requires Liquibase 4.4.3.
* The version of this library now mirrors the version number of the Liquibase version that it requires.

#### 1.0.5

* Added sample for Spring Boot integration
* Bug fix ([#94](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/94)): loadData change did not escape single quotes correctly

#### 1.0.4

* Extension is no longer beta.
* deps: update to jdbc driver version 2.0 and set user agent

#### 1.0.3

* Bug fix ([#83](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/83)): Columns in the primary key were always generated as not nullable, even when they were marked as nullable.
* Bug fix ([#78](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/78)): INTERLEAVED table was generated as FOREIGN KEY in snapshots
* Bug fix ([#75](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/75), [#76](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/76), [#77](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/77)): Wrong type names were generated in snapshots
* Bug fix: Removed logback configuration from build.

#### 1.0.2

 * Bug fix: Fixed a potential `ClassNotFoundException` for `com.google.spanner.admin.database.v1.DatabaseAdminGrpc`

#### 1.0

 * Initial beta release.

## Getting Started

### Installing and setting up Liquibase

Install Liquibase Community CLI from [here](https://www.liquibase.org/). Alternatively, there are many other ways to install (e.g. brew on Mac OS/X).
These examples were run with Liquibase 4.2.0.

Once Liquibase is installed, use the [latest release](https://github.com/cloudspannerecosystem/liquibase-spanner/releases) or build your own
liquibase-spanner-SNAPSHOT-all.jar and copy it into the Liquibase lib directory.

### Starting a Spanner database

You can create a Spanner instance in the [GCP console](https://console.cloud.google.com/spanner/instances/new)
or use an [emulator](https://cloud.google.com/spanner/docs/emulator) (with [JDBC](https://cloud.google.com/spanner/docs/use-oss-jdbc)).
You will also need to create a database for Liquibase to use.

Spanner CLI is a convenient way to access Spanner. It can be installed from [here](https://github.com/cloudspannerecosystem/spanner-cli#install).

Configure the connection in the file liquibase.properties:
```
  url: jdbc:cloudspanner:/projects/<project>/instances/<instance>/databases/<database>
```

### Running Examples

Using the [Liquibase CLI](https://docs.liquibase.com/tools-integrations/cli/home.html) the following ChangeLogs are examples of using Spanner.
Review [Liquibase best practices](https://www.liquibase.org/get-started/best-practices). In this example, [changelog.yaml](example/changelog.yaml)
is used as the master changelog.

Run:
```liquibase --changeLog example/changelog.yaml```

| Example                                                                                    | Description                                                               |
|--------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| [create-schema.yaml](example/create-schema.yaml)                                           | Create schema, including interleaved tables, column options, and indexes  |
| [load-data-singers.yaml](example/load-data-singers.yaml)                                   | Load data into Singers table from CSV                                     | 
| [load-update-data-singers.yaml](example/load-update-data-singers.yaml)                     | Insert or update data in Singers table from CSV                           |
| [add-lookup-table-singers-countries.yaml](example/add-lookup-table-singers-countries.yaml) | Create countries table as a foreign key from Country field in Singers     |
| [modify-data-type-singers-lastname.yaml](example/modify-data-type-singers-lastname.yaml)   | Alter STRING datatype in Singers LastName column                          |
| [insert.yaml](example/insert.yaml)                                                         | Insert rows into Singers table                                            |
| [delete.yaml](example/delete.yaml)                                                         | Delete rows from Singers                                                  |
| [update.yaml](example/update.yaml)                                                         | Update rows in Singers                                                    |

### Other Samples
See the samples directory for specific integrations with other frameworks, such as Spring Boot.

## Supported Features

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:<br/>
createTable, dropTable, addColumn, modifyDataType, addNotNullConstraint, dropColumn, createIndex, dropIndex, addForeignKeyConstraint, dropForeignKeyConstraint, dropAllForeignKeyConstraints, addLookupTable, createView, dropView

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are not allowed with Cloud Spanner:<br/>
addAutoIncrement, addDefaultValue, addPrimaryKey, addUniqueConstraint, dropUniqueConstraint, createProcedure, createSequence, dropDefaultValue, dropNotNullConstraint, dropPrimaryKey, dropProcedure, dropSequence, renameColumn, renameSequence, renameTable, renameView, setColumnRemarks, setTableRemarks, alterSequence

The following data DML [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:<br/>
insert, update, loadData, loadUpdateData

Note:
 * Column OPTIONS and table INTERLEAVE must be applied using modifySql.
 * Instead of unique constraints use unique indexes.

## Limitations

See [limitations.md](limitations.md) for a full list of limitations and unsupported features.

### Spanner-specific SQL

Some Spanner specific SQL, such as INTERLEAVE'd tables or column OPTIONS, require using
Liquibase's modifySql. See [create-schema.yml](example/create-schema.yaml) for an example
of doing this.

### DDL Limits

In order to [limit the number of schema updates in a 7-day period](https://cloud.google.com/spanner/docs/schema-updates#week-window), run
Liquibase with small changeSets. Alternatively, use [SQL change](https://docs.liquibase.com/change-types/community/sql.html) and batch the DDL
using [batch statements](https://cloud.google.com/spanner/docs/jdbc-session-mgmt-commands#batch_statements).

### DML Limits

There are [DML limits](https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data)
for the number of rows affected during DML. The recommendation is to use
[partitioned DML](https://cloud.google.com/spanner/docs/dml-partitioned#dml_and_partitioned_dml).
Using Spanner JDBC driver this can be configured using the
[AUTOCOMMIT_DML_MODE](https://cloud.google.com/spanner/docs/use-oss-jdbc#set_autocommit_dml_mode).

This has been implemented in some of the changeSet types such as mergeColumns, but not in all changeSet types such as
delete.

### Unsupported Spanner Features

There are a number of features that Spanner does not have such as sequences and stored procedures. The Liquibase extension will
throw an exception during analysis of the changeSet in most cases, but not all. For example, a DELETE without a WHERE clause
will fail in Spanner but not in the Liquibase extension.

## Building

### Building

| Gradle target      | Description                                     |
|--------------------|-------------------------------------------------|
| test               | Run mock and Spanner emulator tests             |
| build              | Build extension and run above tests             |
| integrationTest    | Run Spanner in GCP tests                        |
| jibDocker          | Build a local runnable docker container         |

Testing requirements:
 * Emulator requires [testcontainers](https://www.testcontainers.org/) and its [requirements](https://www.testcontainers.org/supported_docker_environment/) installed.
 * Spanner in GCP requires SPANNER_PROJECT and SPANNER_INSTANCE environment variables set to an active instance
 * Spanner in GCP requires application default credentials set or GOOGLE_APPLICATION_CREDENTIALS environent set

### Deploying

There are two JARs built:
 * build/libs/liquibase-spanner-VERSION.jar
 * build/libs/liquibase-spanner-VERSION-all.jar

The first JAR is just the extension itself, while the second one includes all of the dependencies needed to run with Liquibase. Install the second one into the Liquibase lib directory.

## Contributing

For contributions please see [contributing](docs/contributing.md) and our [code of conduct](docs/code-of-conduct.md).

## Raising Issues

If you have any questions, find a bug, or have a feature request please [open an issue](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/new).
Please note that this extension is not officially supported as part of the Cloud Spanner product.
