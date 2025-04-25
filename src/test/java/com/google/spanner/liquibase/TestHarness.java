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

package com.google.spanner.liquibase;

import com.google.cloud.spanner.*;
import com.google.cloud.spanner.connection.ConnectionOptions;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class TestHarness {
  private static final Logger logger = LoggerFactory.getLogger(TestHarness.class);

  /*
   * Interface for a Liquibase JDBC Test Harness
   */
  public interface Connection {

    // Returns the connection URL that is used by this connection.
    String getConnectionUrl();

    // Stop() stops the test Spanner database and does any needed cleanup.
    void stop() throws SQLException;
  }

  private static void createInstance(Spanner service, String instanceId) throws SQLException {

    if (hasInstance(service, instanceId)) {
      logger.info(String.format("Reusing existing instance %s", instanceId));
      return;
    }

    final String projectId = service.getOptions().getProjectId();
    final InstanceId inst = InstanceId.of(projectId, instanceId);

    // Create it.
    try {
      service
          .getInstanceAdminClient()
          .createInstance(
              InstanceInfo.newBuilder(inst)
                  .setDisplayName(" test Instance")
                  .setInstanceConfigId(InstanceConfigId.of(projectId, "emulator-config"))
                  .setNodeCount(1)
                  .build())
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SQLException("Failed creating instance", e);
    }
  }

  private static boolean hasInstance(Spanner service, String instanceId) {
    for (Instance inst : service.getInstanceAdminClient().listInstances().iterateAll()) {
      if (inst.getId().getInstance().equals(instanceId)) return true;
    }
    return false;
  }

  private static boolean hasDatabase(Spanner service, String instanceId, String databaseId) {
    for (Database db : service.getDatabaseAdminClient().listDatabases(instanceId).iterateAll()) {
      if (db.getId().getDatabase().equals(databaseId)) return true;
    }
    return false;
  }

  private static void createDatabase(
      Spanner service, String instanceId, String databaseId, Dialect dialect) throws SQLException {
    if (hasDatabase(service, instanceId, databaseId)) {
      logger.info(
          String.format("Reusing existing instance %s database %s", instanceId, databaseId));
      return;
    }

    try {
      // Create the database
      if (dialect == Dialect.POSTGRESQL) {
        String createDatabaseStatement = String.format("CREATE DATABASE \"%s\"", databaseId);
        service
            .getDatabaseAdminClient()
            .createDatabase(instanceId, createDatabaseStatement, dialect, Collections.emptyList())
            .get();
      } else {
        service
            .getDatabaseAdminClient()
            .createDatabase(instanceId, databaseId, Collections.emptyList())
            .get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new SQLException("Unable to create database", e);
    }

    if (!hasDatabase(service, instanceId, databaseId)) {
      throw new SQLException("Failed creating database! Doesn't exist!");
    }

    logger.info(String.format("Created existing instance %s database %s", instanceId, databaseId));
  }

  private static void dropDatabase(Spanner service, String instanceId, String databaseId)
      throws SQLException {
    service.getDatabaseAdminClient().dropDatabase(instanceId, databaseId);
  }

  static GenericContainer<?> testContainer;
  static Spanner spanner;

  //
  // Create a Spanner emulator instance
  //
  // Creates a temporary Liquibase database using the Spanner emulator
  //
  public static Connection useSpannerEmulator(Dialect dialect) throws SQLException {

    // Test parameters
    final String PROJECT_ID = "test-project-id";
    final String INSTANCE_ID = "test-instance-id";
    final String DATABASE_ID =
        dialect == Dialect.POSTGRESQL ? "test-database-id-pg" : "test-database-id-gsql";

    // Use existing emulator or launch a new one
    String spannerEmulatorHost = System.getenv("SPANNER_EMULATOR_HOST");
    if (spannerEmulatorHost == null && testContainer == null) {

      // Create the container
      final String SPANNER_EMULATOR_IMAGE = "gcr.io/cloud-spanner-emulator/emulator:latest";
      testContainer =
          new GenericContainer<>(SPANNER_EMULATOR_IMAGE)
              .withCommand()
              .withExposedPorts(9010, 9020)
              .waitingFor(Wait.forListeningPort());

      // Start the container
      testContainer.start();
    }
    // JDBC Connection
    spannerEmulatorHost =
        String.format("%s:%d", testContainer.getHost(), testContainer.getMappedPort(9010));

    if (spanner == null) {
      // Create the Spanner service
      spanner =
          SpannerOptions.newBuilder()
              .setProjectId(PROJECT_ID)
              .setEmulatorHost(String.format("http://%s", spannerEmulatorHost))
              .build()
              .getService();
    }
    // Initialize the instance and database.
    createInstance(spanner, INSTANCE_ID);
    createDatabase(spanner, INSTANCE_ID, DATABASE_ID, dialect);

    final String connectionUrl =
        String.format(
            "jdbc:cloudspanner://%s/projects/%s/instances/%s/databases/%s?autocommit=true;usePlainText=true",
            spannerEmulatorHost, PROJECT_ID, INSTANCE_ID, DATABASE_ID);

    return new Connection() {

      @Override
      public String getConnectionUrl() {
        return connectionUrl;
      }

      @Override
      public void stop() throws SQLException {
        if (!spanner.isClosed()) {
          spanner.close();
        }
        try {
          ConnectionOptions.closeSpanner();
        } catch (SpannerException e) {
          // ignore
        }
        if (testContainer != null) {
          testContainer.stop();
        }
      }
    };
  }

  //
  // Create a normal Spanner connection.
  //
  // Automatically creates and drops a temporary liquibase database
  //
  public static Connection useSpannerConnection(Dialect dialect) throws SQLException {
    final String projectId = System.getenv("SPANNER_PROJECT");
    final String instanceId = System.getenv("SPANNER_INSTANCE");
    final String DATABASE_ID =
        dialect == Dialect.POSTGRESQL ? "test-database-id-pg" : "test-database-id-gsql";
    final String databaseId = String.format("%s_%d", DATABASE_ID, new Random().nextInt(1_000_000));

    // Ensure parameters are properly set
    if (projectId == null || instanceId == null) {
      throw new SQLException(
          "Both SPANNER_PROJECT and SPANNER_INSTANCE environment variables must be set!");
    }

    Spanner service = SpannerOptions.newBuilder().setProjectId(projectId).build().getService();

    createDatabase(service, instanceId, databaseId, dialect);

    final String connectionUrl =
        String.format(
            "jdbc:cloudspanner:/projects/%s/instances/%s/databases/%s?autocommit=true",
            projectId, instanceId, databaseId);
    // JDBC connection initialize
    java.sql.Connection conn = DriverManager.getConnection(connectionUrl);

    return new Connection() {

      @Override
      public String getConnectionUrl() {
        return connectionUrl;
      }

      @Override
      public void stop() throws SQLException {
        dropDatabase(service, instanceId, databaseId);
        conn.close();
        service.close();
        try {
          ConnectionOptions.closeSpanner();
        } catch (SpannerException e) {
          // ignore
        }
      }
    };
  }
}
