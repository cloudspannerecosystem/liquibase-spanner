
# Run some examples

## Create the schema

Note this will use some Spanner-specific options when working with Spanner.



liquibase --changeLogFile create-schema.yaml update```

## Add a Country column to the Singers table

```liquibase --changeLogFile add-country-to-singers-table.spanner.yaml```

## Load in some data into the Singers table

```liquibase --changeLogFile load-data-singers.spanner.yaml```

## Update some data in the Singers table.

```liquibase --changeLogFile load-update-data-singers.spanner.yaml```

## Create a Lookup table for Countries

```liquibase --changeLogFile add-lookup-table-singers-countries.spanner.yaml```

## Change the datatype of Singers LastName column

```liquibase --changeLogFile modify-data-type-singers-lastname.spanner.yaml```
