/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.spanner.liquibase.sample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@SpringBootApplication
public class SampleSpringBootApplication {
  /** This sample application uses a Spanner emulator that runs in a Docker container. */
  private static GenericContainer<?> emulator;

  public static void main(String[] args) {
    // Start the Spanner emulator before starting the Spring Boot application.
    System.out.println("Starting Spanner emulator");
    startSpannerEmulator();
    SpringApplication.run(SampleSpringBootApplication.class, args);

    // Stop the emulator and shutdown the application.
    stopSpannerEmulator();
    System.out.println("Stopped Spanner emulator");
    System.exit(0);
  }

  @Bean
  public CommandLineRunner init(DataSource datasource) {
    return args -> {
      // Insert a row in the table that was created by Liquibase and select all rows from the table.
      try (Connection connection = datasource.getConnection()) {
        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (?, ?, ?)")) {
          statement.setString(1, UUID.randomUUID().toString());
          statement.setString(2, "Peter");
          statement.setString(3, "Allison");
          System.out.printf("Inserted %d records\n", statement.executeUpdate());
        }
        try (ResultSet resultSet =
            connection.createStatement().executeQuery("SELECT * FROM Singers")) {
          while (resultSet.next()) {
            System.out.printf("Found singer: %s - %s %s\n", resultSet.getString(1),
                resultSet.getString(2), resultSet.getString(3));
          }
        }
      }
    };
  }

  /**
   * The datasource is dynamically generated, as the mapped port of the emulator on the Docker
   * container is variable.
   */
  @Bean
  public DataSource datasource() {
    DataSourceBuilder<?> builder = DataSourceBuilder.create();
    builder.driverClassName("com.google.cloud.spanner.jdbc.JdbcDriver");
    builder.url(String.format(
        // autoConfigEmulator=true ensures that the connection will use plain text, and it will
        // automatically create the instance and database that is named in the connection string (if
        // they do not already exist).
        "jdbc:cloudspanner://localhost:%d/projects/test-project/instances/test-instance/databases/testdb;autoConfigEmulator=true",
        emulator.getMappedPort(9010)));
    return builder.build();
  }

  @SuppressWarnings("resource")
  static void startSpannerEmulator() {
    String SPANNER_EMULATOR_IMAGE = "gcr.io/cloud-spanner-emulator/emulator:latest";
    emulator = new GenericContainer<>(SPANNER_EMULATOR_IMAGE).withCommand()
        .withExposedPorts(9010, 9020).withStartupTimeout(Duration.ofSeconds(10))
        .waitingFor(Wait.forHttp("/").forStatusCode(404));
    emulator.start();
  }

  static void stopSpannerEmulator() {
    emulator.stop();
  }
}
