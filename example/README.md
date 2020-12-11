
# Getting Started

## Building from source

Build the extension:
```./gradlew shadowJar```

Use the `build/libs/liquibase-extension-SNAPSHOT-all.jar`.

## Installing and setting up Liquibase 

Install Liquibase Community CLI from [here](https://www.liquibase.org/). Alternatively, there are many other ways to install (e.g. brew on Mac OS/X).
These examples were run with Liquibase 4.2.0.

Once Liquibase is installed, the built jar or release jar needs to be copied into the Liquibase lib directory.

## Starting a Spanner database

You can create a Spanner instance in the [GCP console](https://console.cloud.google.com/spanner/instances/new)
or use an [emulator](https://cloud.google.com/spanner/docs/emulator) (with [JDBC](https://cloud.google.com/spanner/docs/use-oss-jdbc)).
You will also need to create to create a database for Liquibase to use.

Spanner CLI is a convenient way to access Spanner. It can be installed from [here](https://github.com/cloudspannerecosystem/spanner-cli#install).

Configure the connection in the file liquibase.properties:
```
  url: jdbc:cloudspanner:/projects/<project>/instances/<instance>/databases/<database>
```

## Run some examples

### Create the Singers table

Note that this will set a column with the auto_commit option if it is Cloud Spanner.

```liquibase --changeLogFile create-singers-table.spanner.yaml update```

### Create the Albums table

Note that this will interleave this in the Singers parent table if it is Cloud Spanner.

```liquibase --changeLogFile create-albums-table.spanner.yaml update```

### Add a Country column to the Singers table

```liquibase --changeLogFile add-country-to-singers-table.spanner.yaml```

### Load in some data into the Singers table

```liquibase --changeLogFile load-data-singers.spanner.yaml```

### Update some data in the Singers table.

```liquibase --changeLogFile load-update-data-singers.spanner.yaml```

### Create a Lookup table for Countries

```liquibase --changeLogFile add-lookup-table-singers-countries.spanner.yaml```

### Index the Singers FirstName column

```liquibase --changeLogFile create-index-singers-first-name.spanner.yaml```

### Change the datatype of Singers LastName column

```liquibase --changeLogFile modify-data-type-singers-lastname.spanner.yaml```
