
# Liquibase Spanner Extension

A Liquibase extension adding support for Google Cloud Spanner. Include this in your
application project to run Liquibase database migration scripts against a Google
Cloud Spanner instance.

## Getting Started

### Installing and setting up Liquibase

Install Liquibase Community CLI from [here](https://www.liquibase.org/). Alternatively, there are many other ways to install (e.g. brew on Mac OS/X).
These examples were run with Liquibase 4.2.0.

Once Liquibase is installed, use the built liquibase-spanner-VERSION-all.jar or release jar needs to be copied into the Liquibase lib directory.

### Starting a Spanner database

You can create a Spanner instance in the [GCP console](https://console.cloud.google.com/spanner/instances/new)
or use an [emulator](https://cloud.google.com/spanner/docs/emulator) (with [JDBC](https://cloud.google.com/spanner/docs/use-oss-jdbc)).
You will also need to create to create a database for Liquibase to use.

Spanner CLI is a convenient way to access Spanner. It can be installed from [here](https://github.com/cloudspannerecosystem/spanner-cli#install).

Configure the connection in the file liquibase.properties:
```
  url: jdbc:cloudspanner:/projects/<project>/instances/<instance>/databases/<database>
```

### Running Examples

Using the [Liquibase CLI](https://docs.liquibase.com/tools-integrations/cli/home.html) the following ChangeLogs are examples of using Spanner.

|-----------------------------------------------------------|---------------------------------------------------------------------------|
| Example                                                   | Description                                                               |
|-----------------------------------------------------------|---------------------------------------------------------------------------|
| [example/create-schema.yaml]                              | Create schema, including interleaved tables, column options, and indexes  |
| [example/load-data-singers.spanner.yaml]                  | Load data into Singers table from CSV                                     | 
| [example/load-update-data-singers.spanner.yaml]           | Insert or update data in Singers table from CSV                           |
| [example/add-lookup-table-singers-countries.spanner.yaml] | Create countries table as a foreign key from Country field in Singers     |
| [example/modify-data-type-singers-lastname.spanner.yaml]  | Convert STRING datatype in Singers LastName column                        |
|-----------------------------------------------------------|---------------------------------------------------------------------------|

## Supported Features

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:
createTable, dropTable, addColumn, modifyDataType, addNotNullConstraint, dropColumn, createIndex, dropIndex, addForeignKeyConstraint, dropForeignKeyConstraint, dropAllForeignKeyConstraints, addLookupTable

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are not allowed with Cloud Spanner:
addAutoIncrement, addDefaultValue, addPrimaryKey, addUniqueConstraint, dropUniqueConstraint, createProcedure, createSequence, createView, dropDefaultValue, dropNotNullConstraint, dropPrimaryKey, dropProcedure, dropSequence, dropView, renameColumn, renameSequence, renameTable, renameView, setColumnRemarks, setTableRemarks

The following data DML [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:
delete, insert, loadData, loadUpdateData

Note:
 * Column OPTIONS and table INTERLEAVE must be applied using modifySql.
 * Instead of addAutoIncrement use allow_commit_timestamp.
 * Instead of unique constraints use unique indexes.

TODO:
 * alterSequence - is this blocked?
 * delete/insert - do these just work?

## Limitations

### Spanner-specific SQL

Some Spanner specific SQL, such as INTERLEAVE'd tables or column OPTIONS, require using
Liquibase's modifySql. See [create-schema.yml](example/create-schema.yml) for an example
of doing this.

### DDL Limits

In order to [limit the number of schema updates in a 7-day period](https://cloud.google.com/spanner/docs/schema-updates#week-window), run
Liquibase with small changeSets. Alternatively, use [SQL change](https://docs.liquibase.com/change-types/community/sql.html) and batch the DDL
using [batch statements](https://cloud.google.com/spanner/docs/use-oss-jdbc#batch_statements).

### DML Limits

There are [DML limits](https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data)
for the number of rows affected during DML. The recommendation is to use
[partitioned DML](https://cloud.google.com/spanner/docs/dml-partitioned#dml_and_partitioned_dml).
Using Spanner JDBC driver this can be configured using the
[AUTOCOMMIT_DML_MODE](https://cloud.google.com/spanner/docs/use-oss-jdbc#set_autocommit_dml_mode).

This has been implemented in some of the changeSet types such as mergeColumns, but not in all changeSet types such as
delete.

### Unsupported Spanner Features

There are a number of features that Spanner does not have such as views and stored procedures. The Liquibase extension will
throw an exception during analysis of the changeSet in most cases, but not all. For example, a DELETE without a WHERE clause
will fail in Spanner but not in the Liquibase extension.

## Release Notes

#### 1.0
* Initial release

## Building and Testing

### Building

Testing is done on three levels:
 * Mock testing of Spanner
 * Spanner emulator using [testcontainers](www.testcontainers.org) (See [requirements](https://www.testcontainers.org/supported_docker_environment/)
 * Spanner in GCP (set SPANNER_PROJECT and SPANNER_INSTANCE)

|--------------------------------|-------------------------------------------------|
| Gradle target                  | Description                                     |
|--------------------------------|-------------------------------------------------|
| `./gradlew test`               | Run mock and Spanner emulator tests             |
| `./gradlew build`              | Build extension and run above tests             |
| `./gradlew integrationTest`    | Run Spanner in GCP tests                        |
| `./gradlew jibDocker`          | Build a local runnable docker container         |
|--------------------------------|-------------------------------------------------|

### Deploying

There are two JARs built:
 * build/libs/liquibase-spanner-<VERSION | SNAPSHOT>.jar
 * build/libs/liquibase-spanner-<VERSION | SNAPSHOT>-all.jar

The first JAR is just the extension itself, while the second one includes all of the dependencies needed to run with Liquibase. Install the second one into the Liquibase lib directory.

## Contributing

For contributions please see [contributing](docs/contributing.md) and our [code of conduct](docs/code-of-conduct.md).

## Issues and Support

If you have any questions or find a bug, please [open an issue](https://github.com/cloudspannerecosystem/liquibase-spanner/issues/new).
Please note that this extension is not officially supported as part of the Cloud Spanner product.
