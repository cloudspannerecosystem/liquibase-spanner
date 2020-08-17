/*
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

import com.github.ajalt.clikt.core.PrintHelpMessage
import com.github.ajalt.clikt.core.subcommands
import com.google.cloud.spanner.InstanceConfigId
import com.google.cloud.spanner.InstanceId
import com.google.cloud.spanner.InstanceInfo
import com.google.cloud.spanner.SpannerOptions
import liquibase.LabelExpression
import org.amshove.kluent.*
import org.spekframework.spek2.Spek
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager
import java.time.Duration
import kotlin.random.Random

class SpecifiedGenericContainer(val image: String) : GenericContainer<SpecifiedGenericContainer>(image)

/**
 * https://medium.com/google-cloud/cloud-spanner-emulator-bf12d141c12
 * https://cloud.google.com/spanner/docs/emulator
 * https://github.com/GoogleCloudPlatform/java-docs-samples/tree/master/spanner/cloud-client
 * https://github.com/GoogleCloudPlatform/cloud-spanner-emulator
 */
fun spannerContainer(): SpecifiedGenericContainer =
    SpecifiedGenericContainer("gcr.io/cloud-spanner-emulator/emulator:latest")
        .withCommand()
        .withExposedPorts(9010, 9020)
        .withStartupTimeout(Duration.ofSeconds(10))
        .waitingFor(Wait.forHttp("/").forStatusCode(404))

/**
 * Return Liquibase utilities for Spanner. By default, use the emulator but if the SPANNER_DATABASE environment
 * variable is populated, use a real instance.
 */
fun spannerUtils(spanner: TestSpannerInstance) = if(System.getenv("SPANNER_DATABASE").isNullOrBlank()) {
    LiquibaseUtils(
        DatabaseType.SPANNER, spanner.jdbcURL(),
        username = null,
        password = null,
        googleCredentials = null,
        changesetFile = File("src/test/resources/changelog.spanner.sql").toPath()
    )
} else {
    LiquibaseUtils(
        DatabaseType.SPANNER, spanner.jdbcURL(),
        username = null,
        password = null,
        googleCredentials = Paths.get(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")!!),
        changesetFile = File("src/test/resources/changelog.spanner.sql").toPath()
    )
}


val sqlComments = Regex("""--.*""")
fun InitialSQL() = File("src/test/resources/initial.spanner.sql").readText().replace(sqlComments, "").split(";")


sealed class TestSpannerInstance() {
    abstract fun jdbcURL(): URI
    abstract fun start(): Unit
    abstract fun stop(): Unit

    class Emulator: TestSpannerInstance() {

        private val emulator = spannerContainer()

        override fun jdbcURL(): URI =
            URI("jdbc:cloudspanner://${emulator.containerIpAddress}:${emulator.getMappedPort(9010)}/projects/your-project-id/instances/test-instance/databases/test?autocommit=false;usePlainText=true")

        override fun start() {
            emulator.start()
            configureSpannerEmulator()
        }

        override fun stop() {
            emulator.stop()
        }

        /**
         * Create a Spanner instance and database inside the emulator docker container.
         */
        fun configureSpannerEmulator() {
            val spannerOptions = SpannerOptions.newBuilder()
                .setProjectId("your-project-id")
                .setEmulatorHost("http://${emulator.containerIpAddress}:${emulator.getMappedPort(9010)}")
                .build()
            val spannerClient = spannerOptions.service
            val instanceId = InstanceId.of(
                spannerOptions.projectId,
                "test-instance"
            )
            spannerClient.instanceAdminClient.createInstance(
                InstanceInfo.newBuilder(instanceId)
                    .setDisplayName("Test Instance")
                    .setInstanceConfigId(InstanceConfigId.of(spannerOptions.projectId, "emulator-config"))
                    .setNodeCount(1)
                    .build()
            ).get()
            spannerClient.databaseAdminClient.createDatabase(
                "test-instance", "test", InitialSQL()
            ).get()
            spannerClient.databaseAdminClient.listDatabases("test-instance").iterateAll().toList().forEach { db ->
                println("Spanner database: $db")
            }

        }

    }

    class Real: TestSpannerInstance() {

        val database: String = "test-database-${Random.nextInt()}"
        val instance = System.getenv("SPANNER_INSTANCE")
        val project = System.getenv("SPANNER_PROJECT")
        val credentialsFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        override fun jdbcURL(): URI =
            URI("""jdbc:cloudspanner:/projects/$project/instances/$instance/databases/$database?autocommit=false;credentials=$credentialsFile""")

        override fun start() {
            configureSpanner()
        }
        override fun stop() { }

        /**
         * Create a Spanner instance and database inside the emulator docker container.
         */
        fun configureSpanner() {
            val spannerOptions = SpannerOptions.newBuilder().setProjectId(project)
                .build()
            spannerOptions.service.use { client ->
                println("Creating new test database: $database")
                client.databaseAdminClient.createDatabase(
                    instance, database, InitialSQL()
                ).get()
                client.databaseAdminClient.listDatabases("test-instance").iterateAll().toList().forEach { db ->
                    println("Spanner database: $db")
                }
            }


        }
    }

}

object LiquibaseTests : Spek({

    group("initialise") {
        val spanner: TestSpannerInstance = if (System.getenv("SPANNER_INSTANCE").isNullOrEmpty()) { TestSpannerInstance.Emulator() } else { TestSpannerInstance.Real() }

        beforeGroup {
            spanner.start()
        }

        test("spanner should accept queries") {
            val SPANNER_JDBC = spanner.jdbcURL().toString()
            println("Connecting to Spanner with JDBC URL: $SPANNER_JDBC")
            DriverManager.getConnection(SPANNER_JDBC).use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT 2").use { rs ->
                        while (rs.next()) {
                            rs.getInt(1) shouldBe 2
                        }
                    }
                }
            }
        }

        test("spanner should update", timeout = 60000) {
            val utils = spannerUtils(spanner)
            utils.update(LabelExpression("base"), tag = "mytag")
            DriverManager.getConnection(spanner.jdbcURL().toString(), null, null).use { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT id, name, extra FROM table1").use { rs ->
                        while (rs.next()) {
                            rs.getInt(1) shouldBe 1
                            rs.getString(2) shouldBeEqualTo "test"
                            rs.getString(3) shouldBeEqualTo "abc"
                        }
                    }

                    statement.executeQuery("SELECT COUNT(*) FROM DATABASECHANGELOG").use { rs ->
                        while (rs.next()) {
                            rs.getInt(1) shouldBe 3
                        }
                    }

                }
            }
        }

        afterGroup {
            // Containers should be automatically stopped
        }
    }
})
