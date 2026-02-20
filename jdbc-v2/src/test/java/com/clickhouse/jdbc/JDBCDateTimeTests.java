package com.clickhouse.jdbc;


import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.DataTypeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
        Instant birthdateInstant = birthdate.atStartOfDay(ZoneOffset.systemDefault()).toInstant();

        Object[] dataset = new Object[]{
                birthdate,
                java.sql.Date.valueOf(birthdate),
                birthdate.format(DataTypeUtils.DATE_FORMATTER)
        };


        Properties props = new Properties();
        props.put(ClientConfigProperties.USE_TIMEZONE.getKey(), "Asia/Tokyo");
        props.put(ClientConfigProperties.serverSetting("session_timezone"), "Asia/Tokyo");
        try (Connection conn = getJdbcConnection(props);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE test_days_before_birthday_party (id Int32, birthdate Date32) Engine MergeTree ORDER BY()");


            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test_days_before_birthday_party VALUES (?, ?)")) {
                for (int i = 0; i < dataset.length; i++) {
                    ps.setInt(1, i + 1);
                    ps.setObject(2, dataset[i]);
                    ps.addBatch();
                }

                ps.executeBatch();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT id, birthdate, birthdate::String, timezone() FROM test_days_before_birthday_party ORDER BY id")) {

                for (int i = 0; i < dataset.length; i++) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), i + 1);

                    java.sql.Date sqlDate = rs.getDate(2);
//                    Instant sqlDateInstant = sqlDate.toInstant(); // throws "operation not supported". why then getTime() ok?!
                    long sqlDateTime = sqlDate.getTime();
                    Assert.assertEquals(sqlDateTime, birthdateInstant.toEpochMilli());

                    LocalDate ld = rs.getObject(2, LocalDate.class);
                    Assert.assertEquals(ld, birthdate);
                    String dateStr = rs.getObject(2, String.class);
                    Assert.assertEquals(dateStr, birthdate.format(DataTypeUtils.DATE_FORMATTER));
                }
            }
        }
    }

    @Test(groups = {"integration"})
    void testDateWithTimezone() throws Exception {

        // 2026-02-01 Midnight in Tokyo as utc timestamp
        long utcTsForDayInTokyo = (1769904000000L) - TimeUnit.HOURS.toMillis(9);
        java.sql.Date dateInTokyo = new Date(utcTsForDayInTokyo);

        java.sql.Date dateToWrite = Date.valueOf("2026-02-01"); // local date
        Calendar tokyoCal = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Asia/Tokyo")));

        LocalDate ldInTokyo = DataTypeUtils.toLocalDate(dateToWrite, tokyoCal.getTimeZone());

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement p = conn.prepareStatement("SELECT toDate(?)")) {
                p.setDate(1, dateToWrite);
                try (ResultSet rs = p.executeQuery()) {
                    Assert.assertTrue(rs.next());

                    // Application MUST use calendar of the same timezone as it for write.
                    java.sql.Date dataFromDb = rs.getDate(1, tokyoCal);

                    // dateToWrite is local date before conversion to Tokyo date.
                    Assert.assertEquals(dataFromDb, dateInTokyo);
                    Assert.assertEquals(dataFromDb.getTime(), utcTsForDayInTokyo);

                    Assert.assertEquals(rs.getObject(1, LocalDate.class), ldInTokyo);
                }
            }
        }
    }

    @Test(groups = {"integration"})
    void testWalkTime() throws Exception {
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
    void testLapsTime() throws Exception {
        if (isVersionMatch("(,25.5]")) {
            return; // time64 was introduced in 25.6
        }

        Properties props = new Properties();
        props.put(ClientConfigProperties.serverSetting("allow_experimental_time_time64_type"), "1");
        try (Connection conn = getJdbcConnection(props);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE TABLE test_laps_time (racerId Int32, lapId Int32, lapTime Time64(3)) Engine MergeTree ORDER BY()");

            Object[][] dataset = new Object[][]{
                    {
                            Duration.of(10, ChronoUnit.SECONDS).plusMillis(123).plusMinutes(1),
                            Duration.of(8, ChronoUnit.SECONDS).plusMillis(456).plusMinutes(1),
                    },
                    {
                            LocalTime.of(0, 3, 50),
                            LocalTime.of(0, 3, 59),
                    },
                    {
                            Duration.of(-100, ChronoUnit.HOURS),
                            Duration.of(-100, ChronoUnit.HOURS),
                    }
            };

            try (PreparedStatement p = conn.prepareStatement("INSERT INTO test_laps_time VALUES (?, ?, ?)")) {
                int racerId = 1;

                for (Object[] row : dataset) {
                    int lapId = 1;
                    for (Object time : row) {
                        p.setInt(1, racerId);
                        p.setInt(2, lapId++);
                        p.setObject(3, time);

                        p.addBatch();
                    }
                    racerId++;
                }

                p.executeBatch();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_laps_time ORDER BY racerId, lapId")) {

                int racerId = 1;

                for (Object[] row : dataset) {
                    int lapId = 1;
                    for (Object time : row) {
                        rs.next();
                        Assert.assertEquals(rs.getInt(1), racerId);
                        Assert.assertEquals(rs.getInt(2), lapId++);
                        Object value = rs.getObject(3, time.getClass());
                        Assert.assertEquals(rs.getObject(3, time.getClass()), time);
                    }
                    racerId++;
                }
            }
        }
    }
}
