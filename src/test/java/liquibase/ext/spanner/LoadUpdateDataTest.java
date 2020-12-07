/**
 * Copyright 2020 Google LLC
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.spanner.v1.ExecuteSqlRequest;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import liquibase.Contexts;
import liquibase.Liquibase;

@Execution(ExecutionMode.SAME_THREAD)
public class LoadUpdateDataTest extends AbstractMockServerTest {
  private static final String INSERT =
      "INSERT INTO Singers (SingerId, Name, Description, SingerInfo, AnyGood, Birthdate, LastConcertTimestamp, ExternalID) "
          + "SELECT @id, @name, @description, @singerinfo, @anygood, @birthdate, @lastconcert, @externalid FROM UNNEST([1]) "
          + "WHERE NOT EXISTS (SELECT SingerId FROM Singers WHERE SingerId = @id)";

  private static final String UPDATE = "UPDATE Singers SET "
      + "AnyGood = @anygood, Birthdate = @birthdate, Description = @description, "
      + "ExternalID = @externalid, LastConcertTimestamp = @lastconcert, "
      + "Name = @name, SingerInfo = @singerinfo WHERE SingerId = @id";

  @BeforeAll
  static void setupResults() throws ParseException {
    Date[] birthdates = new Date[] {Date.fromYearMonthDay(1997, 10, 1),
        Date.fromYearMonthDay(2000, 2, 29), Date.fromYearMonthDay(1980, 12, 1)};
    // Liquibase will always use the default System timezone, so we need to read the timestamps in
    // the local timezone first, and then convert to UTC. This means that the actual date that will
    // be loaded into the database will depend on the timezone of the local system where the update
    // is executed...
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    java.sql.Timestamp[] localConcertDates = new java.sql.Timestamp[] {
        new java.sql.Timestamp(formatter.parse("2019-12-31T10:30:00").getTime()),
        new java.sql.Timestamp(formatter.parse("2020-07-09T22:45:10").getTime()),
        new java.sql.Timestamp(formatter.parse("2018-01-19T01:00:01").getTime()),};
    Timestamp[] concertDates = new Timestamp[] {Timestamp.of(localConcertDates[0]),
        Timestamp.of(localConcertDates[1]), Timestamp.of(localConcertDates[2]),};
    String[] uuids = new String[] {"'5b4beb53-27a6-4b7f-92ac-19c7c95353da'",
        "'9bff9ea5-024c-49b4-8f24-46570e515aff'", "'f1f4c7d2-9ae8-4fdb-94f6-7931736c9cd1'",};

    CloudSpanner db = new CloudSpanner();
    for (int id : new int[] {1, 2, 3}) {
      String insert =
          INSERT.replaceAll("@id", String.valueOf(id)).replaceAll("@name", "'Name " + id + "'")
              .replaceAll("@description", "'Description " + id + "'")
              .replaceAll("@singerinfo", "NULL")
              .replaceAll("@anygood", String.valueOf(id % 2 == 0).toUpperCase())
              .replaceAll("@birthdate",
                  db.getDateLiteral(
                      new java.sql.Date(Date.toJavaUtilDate(birthdates[id - 1]).getTime())))
              .replaceAll("@lastconcert", db.getDateLiteral(concertDates[id - 1].toSqlTimestamp()))
              .replaceAll("@externalid", uuids[id - 1]);
      mockSpanner.putStatementResult(StatementResult.update(Statement.of(insert), 1L));
      String update =
          UPDATE.replaceAll("@id", String.valueOf(id)).replaceAll("@name", "'Name " + id + "'")
              .replaceAll("@description", "'Description " + id + "'")
              .replaceAll("@singerinfo", "NULL")
              .replaceAll("@anygood", String.valueOf(id % 2 == 0).toUpperCase())
              .replaceAll("@birthdate",
                  db.getDateLiteral(
                      new java.sql.Date(Date.toJavaUtilDate(birthdates[id - 1]).getTime())))
              .replaceAll("@lastconcert", db.getDateLiteral(concertDates[id - 1].toSqlTimestamp()))
              .replaceAll("@externalid", uuids[id - 1]);
      mockSpanner.putStatementResult(StatementResult.update(Statement.of(update), 1L));
    }
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testLoadUpdateDataFromYaml() throws Exception {
    for (String file : new String[] {"load-update-data-singers.spanner.yaml"}) {
      try (Connection con = createConnection(); Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }

    Iterable<ExecuteSqlRequest> sqlRequests =
        Iterables.filter(mockSpanner.getRequests(), ExecuteSqlRequest.class);
    Iterator<ExecuteSqlRequest> requests =
        Iterables.filter(sqlRequests, new Predicate<ExecuteSqlRequest>() {
          @Override
          public boolean apply(ExecuteSqlRequest request) {
            return request.getSql().startsWith("INSERT INTO Singers")
                || request.getSql().startsWith("UPDATE Singers");
          }
        }).iterator();
    for (int id : new int[] {1, 2, 3}) {
      assertThat(requests.hasNext()).isTrue();
      ExecuteSqlRequest request = requests.next();
      assertThat(request.getSql()).startsWith("INSERT");
      assertThat(request.getSql())
          .endsWith("WHERE NOT EXISTS (SELECT SingerId FROM Singers WHERE SingerId = " + id + ")");

      assertThat(requests.hasNext()).isTrue();
      request = requests.next();
      assertThat(request.getSql()).startsWith("UPDATE");
      assertThat(request.getSql()).endsWith("WHERE SingerId = " + id);
    }
    assertThat(requests.hasNext()).isFalse();
  }
}
