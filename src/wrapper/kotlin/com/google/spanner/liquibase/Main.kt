/**
 * Copyright 2020 Google LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.spanner.liquibase

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.file
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.change.core.RawSQLChange
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.exception.LiquibaseException
import liquibase.exception.RollbackImpossibleException
import liquibase.ext.spanner.CloudSpanner
import liquibase.resource.FileSystemResourceAccessor
import liquibase.servicelocator.ServiceLocator
import liquibase.statement.SqlStatement
import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.util.*
import liquibase.integration.commandline.Main as LiquibaseMain


private val logger = mu.KotlinLogging.logger {}

/**
 * Database types which this wrapper supports.
 */
enum class DatabaseType() {
    SPANNER
}

/**
 * Container for the SQL relating to a particular change within
 * a changeset.
 */
data class ChangesetSQL(
    val id: String,
    val author: String,
    val changeStatements: List<SqlStatement>,
    val rollbackStatements: List<SqlStatement>
)

/**
 * Utility helper which exposes high level Liquibase functionality to the CLI wrapper. At present,
 * each utility uses a separate database connection, which may be optimised going forwards.
 */
class LiquibaseUtils(
    val databaseType: DatabaseType,
    val url: URI,
    val username: String?,
    val password: String?,
    val googleCredentials: Path?,
    val changesetFile: Path
) {
    /**
     * Connect to the database.
     */
    fun connect(): Connection {
        logger.info { "Connecting to database: $url" }
        return when (databaseType) {
            DatabaseType.SPANNER -> DriverManager.getConnection(
                if (googleCredentials != null) {
                    "$url;credentials=${googleCredentials.toAbsolutePath().toString()}"
                } else {
                    "$url"
                }
            )
        }
    }

    /**
     * Wrapper function to manage liquibase operations which require a database connection.
     */
    fun <R> liquibaseOperation(operation: (liquibase: Liquibase) -> R): R {
        connect().use { connection ->
            val database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))
            val liquibase = Liquibase(changesetFile.toAbsolutePath().toString(), FileSystemResourceAccessor(), database)

            // Manually register Liquibase extensions
            val locater = ServiceLocator.getInstance()
            locater.addPackageToScan("liquibase.ext.spanner")
            DatabaseFactory.getInstance().register(CloudSpanner())

            return operation(liquibase)
        }
    }

    /**
     * Update the database with the change sets matching the provided label expressions,
     * tagging it with the provided tag.
     */
    fun update(labelExpression: LabelExpression, tag: String?) =
        liquibaseOperation { liquibase ->
            logger.info { "Executing Liquibase update..." }
            liquibase.update(null, labelExpression)
            if (tag != null) {
                liquibase.tag(tag)
            }
        }
}

// Delegate main to LiquibaseMain
fun main(args: Array<String>) {
    //print("NO!")
    LiquibaseMain.run(args)
}

