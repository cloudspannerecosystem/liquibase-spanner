
# Liquibase Spanner Extension

A Liquibase extension adding support for Google Cloud Spanner. Include this in your
application project to run Liquibase database migration scripts against a Google
Cloud Spanner database.

## Release Notes

TO-BE-FILLED-IN

## Getting Started

### Installing and setting up Liquibase

Install Liquibase Community CLI from [here](https://www.liquibase.org/). Alternatively, there are many other ways to install (e.g. brew on Mac OS/X).
These examples were run with Liquibase 4.2.0.

Once Liquibase is installed, use the built liquibase-spanner-VERSION-all.jar or release jar needs to be copied into the Liquibase lib directory.

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

## Supported Features

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:<br/>
createTable, dropTable, addColumn, modifyDataType, addNotNullConstraint, dropColumn, createIndex, dropIndex, addForeignKeyConstraint, dropForeignKeyConstraint, dropAllForeignKeyConstraints, addLookupTable

The following Liquibase [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are not allowed with Cloud Spanner:<br/>
addAutoIncrement, addDefaultValue, addPrimaryKey, addUniqueConstraint, dropUniqueConstraint, createProcedure, createSequence, createView, dropDefaultValue, dropNotNullConstraint, dropPrimaryKey, dropProcedure, dropSequence, dropView, renameColumn, renameSequence, renameTable, renameView, setColumnRemarks, setTableRemarks, alterSequence

The following data DML [ChangeTypes](https://docs.liquibase.com/change-types/home.html) are supported:<br/>
insert, update, loadData, loadUpdateData

Note:
 * Column OPTIONS and table INTERLEAVE must be applied using modifySql.
 * Instead of addAutoIncrement use allow_commit_timestamp.
 * Instead of unique constraints use unique indexes.

## Limitations

### Spanner-specific SQL

Some Spanner specific SQL, such as INTERLEAVE'd tables or column OPTIONS, require using
Liquibase's modifySql. See [create-schema.yml](example/create-schema.yaml) for an example
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
