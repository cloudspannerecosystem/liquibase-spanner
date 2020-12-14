/**
 * Copyright 2020 Google LLC
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>https://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.spanner;

import static com.google.common.truth.Truth.assertThat;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.Value;
import com.google.spanner.v1.ExecuteBatchDmlRequest;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.shaded.com.google.common.collect.Iterables;
import liquibase.Contexts;
import liquibase.Liquibase;

@Execution(ExecutionMode.SAME_THREAD)
public class LoadDataTest extends AbstractMockServerTest {
  private static final String INSERT =
      "INSERT INTO Singers(SingerId, Name, Description, SingerInfo, AnyGood, Birthdate, LastConcertTimestamp, ExternalID) "
      + "VALUES(@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8)";
  
  @BeforeAll
  static void setupResults() throws ParseException {
    Date[] birthdates = new Date[] {
        Date.fromYearMonthDay(1997, 10, 1),
        Date.fromYearMonthDay(2000, 2, 29),
        Date.fromYearMonthDay(1980, 12, 1)
    };
    // Liquibase will always use the default System timezone, so we need to read the timestamps in
    // the local timezone first, and then convert to UTC. This means that the actual date that will
    // be loaded into the database will depend on the timezone of the local system where the update
    // is executed...
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    java.sql.Timestamp[] localConcertDates = new java.sql.Timestamp[] {
        new java.sql.Timestamp(formatter.parse("2019-12-31T10:30:00").getTime()),
        new java.sql.Timestamp(formatter.parse("2020-07-09T22:45:10").getTime()),
        new java.sql.Timestamp(formatter.parse("2018-01-19T01:00:01").getTime()),
    };
    Timestamp[] concertDates = new Timestamp[] {
        Timestamp.of(localConcertDates[0]),
        Timestamp.of(localConcertDates[1]),
        Timestamp.of(localConcertDates[2]),
    };
    String[] uuids = new String[] {
        "5b4beb53-27a6-4b7f-92ac-19c7c95353da",
        "9bff9ea5-024c-49b4-8f24-46570e515aff",
        "f1f4c7d2-9ae8-4fdb-94f6-7931736c9cd1",
    };
    
    for (int id : new int[] {1, 2, 3} ) {
      mockSpanner.putStatementResult(StatementResult.update(Statement.newBuilder(INSERT)
          .bind("p1").to(id)
          .bind("p2").to("Name " + id)
          .bind("p3").to("This is a CLOB description " + id)
          .bind("p4").to(ByteArray.copyFrom("singerinfo " + id))
          .bind("p5").to(id % 2 == 0)
          .bind("p6").to(birthdates[id - 1])
          .bind("p7").to(concertDates[id - 1])
          .bind("p8").to(uuids[id - 1])
          .build(), 1L));
    }
  }

  @BeforeEach
  void resetServer() {
    mockSpanner.reset();
    mockAdmin.reset();
  }

  @Test
  void testLoadDataFromYaml() throws Exception {
    for (String file : new String[] {"load-data-singers.spanner.yaml"}) {
      try (Connection con = createConnection();
          Liquibase liquibase = getLiquibase(con, file)) {
        liquibase.update(new Contexts("test"));
      }
    }
    Iterator<ExecuteBatchDmlRequest> requests = Iterables.filter(mockSpanner.getRequests(), ExecuteBatchDmlRequest.class).iterator();
    assertThat(requests.hasNext()).isTrue();
    ExecuteBatchDmlRequest request = requests.next();
    assertThat(requests.hasNext()).isFalse();
    assertThat(request.getStatementsList()).hasSize(3);
    for (int id : new int[] {1, 2, 3}) {
      assertThat(request.getStatements(id - 1).getSql()).isEqualTo(INSERT);
      // INT64 fields are encoded as string values.
      assertThat(request.getStatements(id - 1).getParams().getFieldsMap().get("p1"))
          .isEqualTo(Value.newBuilder().setStringValue(String.valueOf(id)).build());
    }
  }
}
