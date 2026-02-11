package com.clickhouse.jdbc;


import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.client.api.internal.ServerSettings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

@Test(groups = {"integration"})
public class JDBCDateTimeTests extends JdbcIntegrationTest {


    @Test(groups = {"integration"})
    void testDaysBeforeBirthdayParty() throws SQLException {

        LocalDate now = LocalDate.now();
        int daysBeforeParty = 10;
        LocalDate birthdate = now.plusDays(daysBeforeParty);


        Properties props = new Properties();
        props.put(ClientConfigProperties.USE_TIMEZONE.getKey(), "Asia/Tokyo");
        props.put(ClientConfigProperties.serverSetting("session_timezone"), "Asia/Tokyo");
        try (Connection conn = getJdbcConnection(props);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE test_days_before_birthday_party (id Int32, birthdate Date32) Engine MergeTree ORDER BY()");

            final String birthdateStr = birthdate.format(DataTypeUtils.DATE_FORMATTER);
            stmt.executeUpdate("INSERT INTO test_days_before_birthday_party VALUES (1, '" + birthdateStr + "')");

            try (ResultSet rs = stmt.executeQuery("SELECT id, birthdate, birthdate::String, timezone() FROM test_days_before_birthday_party")) {
                Assert.assertTrue(rs.next());

                LocalDate dateFromDb = rs.getObject(2, LocalDate.class);
                Assert.assertEquals(dateFromDb, birthdate);
                Assert.assertEquals(now.toEpochDay() - dateFromDb.toEpochDay(), -daysBeforeParty);
                Assert.assertEquals(rs.getString(4), "Asia/Tokyo");


                Assert.assertEquals(rs.getString(2), rs.getString(3));

                java.sql.Date sqlDate = rs.getDate(2); // in local timezone

                String zoneId = "Asia/Tokyo";
                Calendar tzCalendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(zoneId))); // TimeZone.getTimeZone() doesn't throw exception but fallback to GMT
                java.sql.Date tzSqlDate = rs.getDate(2, tzCalendar); // Calendar tells from what timezone convert to local
                Assert.assertEquals(Math.abs(sqlDate.toLocalDate().toEpochDay() - tzSqlDate.toLocalDate().toEpochDay()), 1,
                        "tzCalendar " + tzCalendar + " default " + Calendar.getInstance().getTimeZone().getID());
            }
        }
    }

    @Test(groups = {"integration"})
    void testWalkTime() throws SQLException {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }
        int hours = 100;
        Duration walkTime = Duration.ZERO.plusHours(hours).plusMinutes(59).plusSeconds(59).plusMillis(300);
        System.out.println(walkTime);

        Properties props = new Properties();
        props.put(ClientConfigProperties.USE_TIMEZONE.getKey(), "Asia/Tokyo");
        props.put(ClientConfigProperties.serverSetting("session_timezone"), "Asia/Tokyo");
        props.put(ClientConfigProperties.serverSetting("allow_experimental_time_time64_type"), "1");
        try (Connection conn = getJdbcConnection(props);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE test_walk_time (id Int32, walk_time Time64(3)) Engine MergeTree ORDER BY()");

            final String walkTimeStr = DataTypeUtils.durationToTimeString(walkTime, 3);
            System.out.println(walkTimeStr);
            stmt.executeUpdate("INSERT INTO test_walk_time VALUES (1, '" + walkTimeStr + "')");

            try (ResultSet rs = stmt.executeQuery("SELECT id, walk_time, walk_time::String, timezone() FROM test_walk_time")) {
                Assert.assertTrue(rs.next());

                LocalTime dbTime = rs.getObject(2, LocalTime.class);
                Assert.assertEquals(dbTime.getHour(), hours % 24); // LocalTime is only 24 hours and will truncate big hour values
                Assert.assertEquals(dbTime.getMinute(), 59);
                Assert.assertEquals(dbTime.getSecond(), 59);
                Assert.assertEquals(dbTime.getNano(), TimeUnit.MILLISECONDS.toNanos(300));

                LocalDateTime utDateTime = rs.getObject(2, LocalDateTime.class); // LocalDateTime covers all range
                Assert.assertEquals(utDateTime.getYear(), 1970);
                Assert.assertEquals(utDateTime.getMonth(), Month.JANUARY);
                Assert.assertEquals(utDateTime.getDayOfMonth(), 1 + (hours / 24));

                Assert.assertEquals(utDateTime.getHour(), walkTime.toHours() % 24); // LocalTime is only 24 hours and will truncate big hour values
                Assert.assertEquals(utDateTime.getMinute(), 59);
                Assert.assertEquals(utDateTime.getSecond(), 59);
                Assert.assertEquals(utDateTime.getNano(), TimeUnit.MILLISECONDS.toNanos(300));

                Duration dbDuration = rs.getObject(2, Duration.class);
                Assert.assertEquals(dbDuration, walkTime);

                java.sql.Time sqlTime = rs.getTime(2);
                Assert.assertEquals(sqlTime.toLocalTime(), dbTime.truncatedTo(ChronoUnit.SECONDS)); // java.sql.Time accepts milliseconds but converts to LD with seconds precision.
            }
        }
    }

    @Test(groups = {"integration"})
    void testEventTimestamp() throws SQLException {

        Properties config = new Properties();
//        config.setProperty(ClientConfigProperties.serverSetting("session_timezone"), "America/Los_Angeles");
        try (Connection conn = getJdbcConnection(config)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test_events_with_timestamp");
                stmt.execute("CREATE TABLE test_events_with_timestamp ( " +
                        "id Int32, " +
                        "utc_ts DateTime64(9, 'UTC'), " + // local datetime will be written as UTC
                        "ts_wo_tz DateTime64(9), " +
                        ") Engine MergeTree order by ()");


            }

            LocalDateTime nowLDT = LocalDateTime.now();
            Timestamp nowTs = Timestamp.valueOf(nowLDT);

            Object[][] dataset = new Object[][]{
                    {1, nowTs, nowTs},
                    {2, nowLDT, nowLDT}
            };


            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_events_with_timestamp VALUES (?, ?, ?)")) {
                for (Object[] row : dataset) {
                    for (int colI = 1; colI <= dataset[0].length; colI++) {
                        stmt.setObject(colI, row[colI - 1]);
                    }
                    stmt.addBatch();
                }

                stmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM test_events_with_timestamp ORDER BY id")) {

                int id = 0;
                while (rs.next()) {
                    for (int i = 2; i <= dataset[0].length; i++) { // date time column start with second one
                        Object expected = dataset[id][i - 1];
                        String failMsg = " ts do not match id = " + id + ", col = " + i;
                        if (expected instanceof Timestamp) {
                            Assert.assertEquals(rs.getTimestamp(i), dataset[id][i - 1], failMsg);
                        } else {
                            Assert.assertEquals(rs.getObject(i, expected.getClass()), expected, failMsg);
                        }
                    }
                    ++id;
                    Assert.assertEquals(rs.getInt(1), id);
                }

                Assert.assertEquals(id, dataset.length);
            }
        }
    }

}
