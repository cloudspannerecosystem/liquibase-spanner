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

# ShadowJar
The ShadowJar contains the extension, Spanner JDBC Driver, and dependencies. This can be included
in the classpath of Liquibase the CLI for interacting with Spanner databases. See run.sh for an
example of using this.

# Docker container
The docker container contains the contents of the ShadowJar and Liquibase itself. This is a runnable
container that embeds everything you need to run Liquibase. See run.sh for an example of using
this.

# Supported Features

Status meaning -
 * Checked means it is a supported and tested feature
 * Cross means it is not supported and will throw an error
 * No status means it is untested

| Change Type                      | Status  |
| -------------------------------- |:-------:|
| addAutoIncrement                 | &cross; |                          
| addColumn                        | &check; |                   
| addDefaultValue                  | &cross; |                         
| addForeignKeyConstraint          | &check; |                                 
| addLookupTable                   | &check; |                        
| addNotNullConstraint             | ??      |                              
| addPrimaryKey                    | &cross; |                       
| addUniqueConstraint              | &cross; |                             
| createIndex                      | &check; |                     
| createProcedure                  | To Stop |                         
| createSequence                   | &cross; |                        
| createTable                      | &check; |                     
| createView                       | &cross; |                    
| dropAllForeignKeyConstraints     | &check; |                                      
| dropColumn                       | &check; |                    
| dropDefaultValue                 | &cross; |                          
| dropForeignKeyConstraint         | To Test |                                  
| dropIndex                        | &check; |                   
| dropNotNullConstraint            | &cross; |                               
| dropPrimaryKey                   | &cross; |                        
| dropProcedure                    | To Stop |                       
| dropSequence                     | &cross; |
| dropTable                        | &check; |                   
| dropUniqueConstraint             | &cross; |                              
| dropView                         | &cross; |
| renameColumn                     | &cross; |                      
| renameSequence                   | &cross; |                        
| renameTable                      | &cross; |                     
| renameView                       | &cross; |                    
| sql                              | N/A     |             
| sqlFile                          | N/A     |                 
| alterSequence                    | To Stop |                       
| customChange                     | N/A     |                      
| delete                           | To Test |                
| executeCommand                   | N/A     |                        
| insert                           | To Test |                
| loadData                         | To Test |                  
| loadUpdateData                   | To Test |                        
| mergeColumns                     | To Test |                      
| modifyDataType                   | To Stop |                        
| output                           | N/A     |                
| setColumnRemarks                 | ??      |                          
| setTableRemarks                  | ??      |                         
| stop                             | N/A     |              
| tagDatabase                      | N/A     |                     
| update                           | To Test |                


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

