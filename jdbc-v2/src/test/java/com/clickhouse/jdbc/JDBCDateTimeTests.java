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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    void testTimezoneInSeparateColumn() throws SQLException {

        final Properties config = new Properties();
        String sessionTimezone = "UTC";
        config.setProperty(ServerSettings.ConfigProperties.SESSION_TZ_SETTING, sessionTimezone);
        try (Connection conn = getJdbcConnection(config)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test_tz_column");
                stmt.execute("CREATE TABLE test_events_with_timestamp ( " +
                        "id Int32, " +
                        "ts DateTime64(9), " +
                        "tz String, " +
                        ") Engine MergeTree order by ()");



            }

            Calendar localCalendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("Europe/Amsterdam")));
            localCalendar.set(2026, Month.JANUARY.getValue() - 1, 8, 10, 30, 0);
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_events_with_timestamp VALUES (?, ?, ?)")) {
                Calendar cal;
                Timestamp ts;
                int i =0;

                // save local time as UTC
                stmt.setInt(1, ++i);
                cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")));
                ts = new Timestamp(localCalendar.getTimeInMillis());
                stmt.setTimestamp(2, ts, cal); // local time in utc.
                stmt.setString(3, cal.getTimeZone().getID());

                stmt.addBatch();

                // save local time as is
                stmt.setInt(1, ++i);
                cal = localCalendar;
                ts = new Timestamp(localCalendar.getTimeInMillis());
                stmt.setTimestamp(2, ts, cal); // local time in utc.
                stmt.setString(3, cal.getTimeZone().getID());

                stmt.addBatch();

                stmt.executeBatch();
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT *, toUnixTimestamp(ts) unix_ts FROM test_events_with_timestamp ORDER BY id")) {
                Calendar cal;
                Timestamp ts;


                // local time in UTC
                Assert.assertTrue(rs.next());
                cal = localCalendar;
//                cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(rs.getString(3))));
                ts = rs.getTimestamp(2, cal);
                long unixTs = rs.getLong("unix_ts");
                System.out.println(ts + " " + cal.getTimeZone().toZoneId() + " (" + unixTs + ")");

                // local time in local timezone
                Assert.assertTrue(rs.next());
                cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of(rs.getString(3))));
                ts = rs.getTimestamp(2, cal);
                System.out.println(ts + " " + cal.getTimeZone().toZoneId() + " (" + unixTs + ")");
            }
        }

    }


    @Test(groups = {"integration"})
    void testTimezoneInSeparateColumnZonedDateTime() throws SQLException {
        ZonedDateTime localTimestamp = ZonedDateTime.parse("2026-01-08T10:30:00+09:00[Asia/Tokyo]");

        final Properties config = new Properties();
        String sessionTimezone = "UTC";
        config.setProperty(ServerSettings.ConfigProperties.SESSION_TZ_SETTING, sessionTimezone);
        try (Connection conn = getJdbcConnection(config)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test_tz_col_zoned_datetime");
                stmt.execute("CREATE TABLE test_tz_col_zoned_datetime ( " +
                        "id Int32, " +
                        "ts DateTime64(9), " +
                        "tz String, " +
                        ") Engine MergeTree order by ()");



            }

            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_tz_col_zoned_datetime VALUES (?, ?, ?)")) {
                Calendar cal;
                Timestamp ts;
                int i =0;

                // save local time as UTC
                stmt.setInt(1, ++i);
                stmt.setObject(2, localTimestamp);
                stmt.setString(3, localTimestamp.getZone().getId());
                stmt.addBatch();

                stmt.executeBatch();
            }


            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM test_tz_col_zoned_datetime ORDER BY id")) {
                ZonedDateTime zdt;
                String timezone;


                Assert.assertTrue(rs.next());


                timezone = rs.getString("tz");
                Assert.assertEquals(timezone, localTimestamp.getZone().getId());
                zdt = rs.getObject(2, ZonedDateTime.class);
                Assert.assertEquals(zdt.withZoneSameInstant(ZoneId.of(timezone)), localTimestamp);
            }
        }

        // read with another session timezone
        config.setProperty(ServerSettings.ConfigProperties.SESSION_TZ_SETTING, "Asia/Tokyo");
        try (Connection conn = getJdbcConnection(config)) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM test_tz_col_zoned_datetime ORDER BY id")) {
                ZonedDateTime zdt;
                String timezone;


                Assert.assertTrue(rs.next());


                timezone = rs.getString("tz");
                Assert.assertEquals(timezone, localTimestamp.getZone().getId());
                zdt = rs.getObject(2, ZonedDateTime.class);
                System.out.println(zdt);
                Assert.assertEquals(zdt.withZoneSameInstant(ZoneId.of(timezone)), localTimestamp);
            }

        }
    }


    @Test(groups = {"integration"})
    void testEventTimestamp() throws SQLException {
        try (Connection conn = getJdbcConnection()) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS test_events_with_timestamp");
                stmt.execute("CREATE TABLE test_events_with_timestamp ( " +
                        "id Int32, " +
                        "ts DateTime64(9, 'UTC'), " + // local datetime will be written as UTC
                        "ts_alt DateTime64(9), " +
                        ") Engine MergeTree order by ()");


            }
        }

        {
            final Properties config = new Properties();
            String sessionTimezone = "Asia/Tokyo";
            config.setProperty(ServerSettings.ConfigProperties.SESSION_TZ_SETTING, sessionTimezone);

            try (Connection conn = getJdbcConnection(config)) {
                Instant utcTime = Instant.parse("2026-01-08T23:59:59Z");
                Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                System.out.println("utcTime instant: " + utcTime);

                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_events_with_timestamp VALUES (?, ?, ?)");
                     Statement stmt2 = conn.createStatement()) {

                    stmt.setInt(1, 1);
                    stmt.setTimestamp(2, Timestamp.from(utcTime), utcCalendar); // utc ts string
                    stmt.setTimestamp(3, Timestamp.from(utcTime));

                    stmt.execute();

                    try (ResultSet rs = stmt2.executeQuery("SELECT * " +
                            " , timezone() tz" +
                            " FROM test_events_with_timestamp ORDER BY id")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(rs.getString("tz"), sessionTimezone);

                        // No session_timezone effect because column defines timezone
                        ZonedDateTime zTs = rs.getObject("ts", ZonedDateTime.class);
                        Assert.assertEquals(zTs, ZonedDateTime.parse("2026-01-08T23:59:59Z[UTC]"));
                        System.out.println(zTs);

                        // session_timezone = 'Asia/Tokyo' and time for ts_alt should be in this timezone
                        ZonedDateTime zTsAlt = rs.getObject("ts_alt", ZonedDateTime.class);
                        System.out.println(zTsAlt);
                        Assert.assertEquals(zTsAlt, ZonedDateTime.parse("2026-01-09T08:59:59+09:00[Asia/Tokyo]"));

                        Timestamp ts = rs.getTimestamp("ts", utcCalendar);
                        Assert.assertEquals(ts.toInstant(), zTs.toInstant());
                        Timestamp tsAlt = rs.getTimestamp("ts_alt");
                        Assert.assertEquals(tsAlt.toInstant(), zTsAlt.toInstant());

                    }
                }
            }
        }

        // UTC session
        {
            final Properties config = new Properties();
            String sessionTimezone = "UTC";
            config.setProperty(ServerSettings.ConfigProperties.SESSION_TZ_SETTING, sessionTimezone);

            try (Connection conn = getJdbcConnection(config)) {
                Instant utcTime = Instant.parse("2026-01-08T23:59:59Z");
                Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                System.out.println("utcTime instant: " + utcTime);

                try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO test_events_with_timestamp VALUES (?, ?, ?)");
                     Statement stmt2 = conn.createStatement()) {

                    stmt.setInt(1, 1);
                    stmt.setTimestamp(2, Timestamp.from(utcTime), utcCalendar); // utc ts string
                    stmt.setTimestamp(3, Timestamp.from(utcTime)); // in UTC too

                    stmt.execute();

                    try (ResultSet rs = stmt2.executeQuery("SELECT * " +
                            " , timezone() tz" +
                            " FROM test_events_with_timestamp ORDER BY id")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(rs.getString("tz"), sessionTimezone);

                        // -- Row 1
                        // No session_timezone effect because column defines timezone
                        ZonedDateTime zTs = rs.getObject("ts", ZonedDateTime.class);
                        Assert.assertEquals(zTs, ZonedDateTime.parse("2026-01-08T23:59:59Z[UTC]"));

                        ZonedDateTime zTsAlt = rs.getObject("ts_alt", ZonedDateTime.class);
                        // now we are in UTC session
                        Assert.assertEquals(zTsAlt, ZonedDateTime.parse("2026-01-08T23:59:59Z[UTC]"));

                        Timestamp ts = rs.getTimestamp("ts", utcCalendar);
                        Assert.assertEquals(ts.toInstant(), zTs.toInstant());
                        Timestamp tsAlt = rs.getTimestamp("ts_alt");
                        Assert.assertEquals(tsAlt.toInstant(), zTsAlt.toInstant());

                        // -- Row 2
                        Assert.assertTrue(rs.next());

                        // No session_timezone effect because column defines timezone
                        zTs = rs.getObject("ts", ZonedDateTime.class);
                        Assert.assertEquals(zTs, ZonedDateTime.parse("2026-01-08T23:59:59Z[UTC]"));

                        zTsAlt = rs.getObject("ts_alt", ZonedDateTime.class);
                        // now we are in UTC session
                        Assert.assertEquals(zTsAlt, ZonedDateTime.parse("2026-01-08T23:59:59Z[UTC]"));

                        ts = rs.getTimestamp("ts", utcCalendar);
                        Assert.assertEquals(ts.toInstant(), zTs.toInstant());
                        tsAlt = rs.getTimestamp("ts_alt");
                        Assert.assertEquals(tsAlt.toInstant(), zTsAlt.toInstant());
                    }
                }

            }
        }

    }
}
