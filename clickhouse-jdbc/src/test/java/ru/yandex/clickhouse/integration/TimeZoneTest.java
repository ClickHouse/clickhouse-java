package ru.yandex.clickhouse.integration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.JdbcIntegrationTest;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import static org.testng.Assert.fail;

public class TimeZoneTest extends JdbcIntegrationTest {

    private ClickHouseConnection connectionServerTz;
    private ClickHouseConnection connectionManualTz;

    private long currentTime = 1000 * (System.currentTimeMillis() / 1000);

    @BeforeClass(groups = "integration")
    public void setUp() throws Exception {
        connectionServerTz = newConnection();

        TimeZone serverTimeZone = connectionServerTz.getTimeZone();
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUseServerTimeZone(false);
        int serverTimeZoneOffsetHours = (int) TimeUnit.MILLISECONDS.toHours(serverTimeZone.getOffset(currentTime));
        int manualTimeZoneOffsetHours = serverTimeZoneOffsetHours - 1;
        properties.setUseTimeZone("GMT" + (manualTimeZoneOffsetHours > 0 ? "+" : "")  + manualTimeZoneOffsetHours + ":00");
        connectionManualTz = newConnection(properties);
    }

    @AfterClass(groups = "integration")
    public void tearDown() throws Exception {
        closeConnection(connectionServerTz);
        closeConnection(connectionManualTz);
    }

    @Test(groups = "integration")
    public void timeZoneTestTimestamp() throws Exception {
        try (Statement s = connectionServerTz.createStatement()) {
            s.execute("DROP TABLE IF EXISTS time_zone_test");
            s.execute(
                "CREATE TABLE IF NOT EXISTS time_zone_test (i Int32, d DateTime) ENGINE = TinyLog"
            );
        }

        try (PreparedStatement statement =
            connectionServerTz.prepareStatement("INSERT INTO time_zone_test (i, d) VALUES (?, ?)")) {
            statement.setInt(1, 1);
            statement.setTimestamp(2, new Timestamp(currentTime));
            statement.execute();
        }

        try (PreparedStatement statementUtc =
            connectionManualTz.prepareStatement("INSERT INTO time_zone_test (i, d) VALUES (?, ?)")) {
            statementUtc.setInt(1, 2);
            statementUtc.setTimestamp(2, new Timestamp(currentTime));
            statementUtc.execute();
        }

        String query = "SELECT i, d as cnt from time_zone_test order by i";
        try (Statement s = connectionServerTz.createStatement(); ResultSet rs = s.executeQuery(query)) {
            // server write, server read
            rs.next();
            Assert.assertEquals(rs.getTimestamp(2).getTime(), currentTime);

            // manual write, server read
            rs.next();
            Assert.assertEquals(rs.getTimestamp(2).getTime(), currentTime - TimeUnit.HOURS.toMillis(1));
        }

        try (Statement s = connectionManualTz.createStatement(); ResultSet rsMan = s.executeQuery(query)) {
            // server write, manual read
            rsMan.next();
            Assert.assertEquals(rsMan.getTimestamp(2).getTime(), currentTime);
            // manual write, manual read
            rsMan.next();
            Assert.assertEquals(rsMan.getTimestamp(2).getTime(), currentTime - TimeUnit.HOURS.toMillis(1));
        }
    }

    /*
     * These are the scenarios we need to test
     *
     * USE_SERVER_TIME_ZONE  USE_TIME_ZONE  USE_SERVER_TIME_ZONE_FOR_DATES  effective TZ Date parsing
     * --------------------  -------------  ------------------------------  -------------------------
     * false                 null           false                           (forbidden)
     * false                 TZ_DIFF        false                           TZ_JVM
     * false                 null           true                            (forbidden)
     * false                 TZ_DIFF        true                            TZ_DIFF
     * true                  null           false                           TZ_JVM
     * true                  TZ_DIFF        false                           (forbidden)
     * true                  null           true                            TZ_SERVER
     * true                  TZ_DIFF        true                            (forbidden)
     */

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate1() throws Exception {
        try {
            createConnection(false, null, false);
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate2() throws Exception {
        int suffix = 2;
        Long offset = Long.valueOf(
            TimeUnit.MILLISECONDS.toHours(
                connectionServerTz.getServerTimeZone().getOffset(currentTime)));
        resetDateTestTable(suffix);
        try (ClickHouseConnection conn = createConnection(false, offset, false)) {
            insertDateTestData(suffix, connectionServerTz, 1);
            insertDateTestData(suffix, conn, 1);

            assertDateResult(suffix, conn, Instant.ofEpochMilli(currentTime)
                .atOffset(ZoneOffset.ofHours(offset.intValue()))
                .truncatedTo(ChronoUnit.DAYS)
                .toEpochSecond());
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate3() throws Exception {
        try {
            createConnection(false, null, false);
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate4() throws Exception {
        int suffix = 4;
        Long offset = Long.valueOf(
            TimeUnit.MILLISECONDS.toHours(
                connectionServerTz.getTimeZone().getOffset(currentTime)));
        resetDateTestTable(suffix);
        try (ClickHouseConnection conn = createConnection(false, offset, true)) {
            insertDateTestData(suffix, connectionServerTz, 1);
            insertDateTestData(suffix, conn, 1);
            assertDateResult(
                suffix, conn,
                Instant.ofEpochMilli(currentTime)
                    .atOffset(ZoneOffset.ofHours(offset.intValue()))
                    .truncatedTo(ChronoUnit.DAYS)
                    .toEpochSecond());
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate5() throws Exception {
        int suffix = 5;
        resetDateTestTable(suffix);
        try (ClickHouseConnection conn = createConnection(true, null, false)) {
            insertDateTestData(suffix, connectionServerTz, 1);
            insertDateTestData(suffix, conn, 1);
        
            assertDateResult(
                suffix, conn,
                Instant.ofEpochMilli(currentTime)
                    .atZone(ZoneId.systemDefault())
                    .truncatedTo(ChronoUnit.DAYS)
                    .toEpochSecond());
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate6() throws Exception {
        try {
            createConnection(true, Long.valueOf(1L), false);
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate7() throws Exception {
        int suffix = 7;
        resetDateTestTable(suffix);
        try (ClickHouseConnection conn = createConnection(true, null, true)) {
            insertDateTestData(suffix, connectionServerTz, 1);
            insertDateTestData(suffix, conn, 1);
            assertDateResult(
                suffix, conn,
                Instant.ofEpochMilli(currentTime)
                    .atZone(ZoneId.systemDefault())
                    .truncatedTo(ChronoUnit.DAYS)
                    .toEpochSecond());
        }
    }

    @Test(groups = "integration")
    public void testTimeZoneParseSQLDate8() throws Exception {
        try {
            createConnection(true, Long.valueOf(1L), true);
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test(groups = "integration")
    public void dateTest() throws SQLException {
        ResultSet rsMan = connectionManualTz.createStatement().executeQuery("select toDate('2017-07-05')");
        ResultSet rsSrv = connectionServerTz.createStatement().executeQuery("select toDate('2017-07-05')");
        rsMan.next();
        rsSrv.next();
        // check it doesn't depend on server timezone
        Assert.assertEquals(rsMan.getDate(1), rsSrv.getDate(1));

        // check it is start of correct day in client timezone
        Assert.assertEquals(rsMan.getDate(1), new Date(117, 6, 5));
    }

    @Test(groups = "integration")
    public void dateInsertTest() throws SQLException {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS date_insert");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE date_insert (" +
                        "i UInt8," +
                        "d Date" +
                        ") ENGINE = TinyLog"
        );


        final Date date = new Date(currentTime);
        Date localStartOfDay = withTimeAtStartOfDay(date);

        connectionServerTz.createStatement().sendRowBinaryStream(
                "INSERT INTO date_insert (i, d)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                            stream.writeUInt8(1);
                            stream.writeDate(date);
                    }
                }
        );

        connectionManualTz.createStatement().sendRowBinaryStream(
                "INSERT INTO date_insert (i, d)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        stream.writeUInt8(2);
                        stream.writeDate(date);
                    }
                }
        );

        ResultSet rsMan = connectionManualTz.createStatement().executeQuery("select d from date_insert order by i");
        ResultSet rsSrv = connectionServerTz.createStatement().executeQuery("select d from date_insert order by i");
        rsMan.next();
        rsSrv.next();
        // inserted in server timezone
        Assert.assertEquals(rsMan.getDate(1), localStartOfDay);
        Assert.assertEquals(rsSrv.getDate(1), localStartOfDay);

        rsMan.next();
        rsSrv.next();
        // inserted in manual timezone
        Assert.assertEquals(rsMan.getDate(1), localStartOfDay);
        Assert.assertEquals(rsSrv.getDate(1), localStartOfDay);
    }

    @Test(groups = "integration")
    public void testParseColumnsWithDifferentTimeZones() throws Exception {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS fun_with_timezones");
        connectionServerTz.createStatement().execute(
            "CREATE TABLE fun_with_timezones (" +
                "i         UInt8," +
                "dt_server DateTime," +
                "dt_berlin DateTime('Europe/Berlin')," +
                "dt_lax    DateTime('America/Los_Angeles')" +
            ") ENGINE = TinyLog"
        );
        connectionServerTz.createStatement().execute(
            "INSERT INTO fun_with_timezones (i, dt_server, dt_berlin, dt_lax) " +
            "VALUES (42, 1557136800, 1557136800, 1557136800)");
        ResultSet rs_timestamps = connectionServerTz.createStatement().executeQuery(
            "SELECT i, toUnixTimestamp(dt_server), toUnixTimestamp(dt_berlin), toUnixTimestamp(dt_lax) " +
            "FROM fun_with_timezones");
        rs_timestamps.next();
        Assert.assertEquals(rs_timestamps.getLong(2), 1557136800);
        Assert.assertEquals(rs_timestamps.getLong(3), 1557136800);
        Assert.assertEquals(rs_timestamps.getLong(4), 1557136800);

        ResultSet rs_datetimes = connectionServerTz.createStatement().executeQuery(
            "SELECT i, dt_server, dt_berlin, dt_lax " +
            "FROM fun_with_timezones");
        rs_datetimes.next();
        Assert.assertEquals(rs_datetimes.getTimestamp(2).getTime(), 1557136800000L);
        Assert.assertEquals(rs_datetimes.getTimestamp(3).getTime(), 1557136800000L);
        Assert.assertEquals(rs_datetimes.getTimestamp(4).getTime(), 1557136800000L);
    }

    @Test(groups = "integration")
    public void testParseColumnsWithDifferentTimeZonesArray() throws Exception {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS fun_with_timezones_array");
        connectionServerTz.createStatement().execute(
            "CREATE TABLE fun_with_timezones_array (" +
                "i         UInt8," +
                "dt_server Array(DateTime)," +
                "dt_berlin Array(DateTime('Europe/Berlin'))," +
                "dt_lax    Array(DateTime('America/Los_Angeles'))" +
            ") ENGINE = TinyLog"
        );
        connectionServerTz.createStatement().execute(
            "INSERT INTO fun_with_timezones_array (i, dt_server, dt_berlin, dt_lax) " +
            "VALUES (42, [1557136800], [1557136800], [1557136800])");
        ResultSet rs_timestamps = connectionServerTz.createStatement().executeQuery(
            "SELECT i, array(toUnixTimestamp(dt_server[1])), array(toUnixTimestamp(dt_berlin[1])), array(toUnixTimestamp(dt_lax[1])) " +
            "FROM fun_with_timezones_array");
        rs_timestamps.next();
        Assert.assertEquals(((long[]) rs_timestamps.getArray(2).getArray())[0], 1557136800);
        Assert.assertEquals(((long[]) rs_timestamps.getArray(3).getArray())[0], 1557136800);
        Assert.assertEquals(((long[]) rs_timestamps.getArray(4).getArray())[0], 1557136800);

        ResultSet rs_datetimes = connectionServerTz.createStatement().executeQuery(
            "SELECT i, dt_server, dt_berlin, dt_lax " +
            "FROM fun_with_timezones_array");
        rs_datetimes.next();
        Assert.assertEquals(((Timestamp[]) rs_datetimes.getArray(2).getArray())[0].getTime(), 1557136800000L);
        Assert.assertEquals(((Timestamp[]) rs_datetimes.getArray(3).getArray())[0].getTime(), 1557136800000L);
        Assert.assertEquals(((Timestamp[]) rs_datetimes.getArray(4).getArray())[0].getTime(), 1557136800000L);
    }

    private static Date withTimeAtStartOfDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    private ClickHouseConnection createConnection(boolean useServerTimeZone,
        Long manualTimeZoneOffsetHours, boolean useServerTimeZoneForParsingDates) throws SQLException
    {
        ClickHouseProperties props = new ClickHouseProperties();
        props.setUseServerTimeZone(useServerTimeZone);
        if (manualTimeZoneOffsetHours != null) {
            props.setUseTimeZone(
                "GMT"
              + (manualTimeZoneOffsetHours.intValue() > 0 ? "+" : "")
              + manualTimeZoneOffsetHours + ":00");
        }
        props.setUseServerTimeZoneForDates(useServerTimeZoneForParsingDates);
        return newConnection(props);
    }

    private void resetDateTestTable(int suffix) throws Exception {
        connectionServerTz.createStatement().execute(
            "DROP TABLE IF EXISTS time_zone_test" + suffix);
        connectionServerTz.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS time_zone_test" + suffix + " (i Int32, d DateTime) ENGINE = TinyLog"
        );
    }

    private void insertDateTestData(int suffix, ClickHouseConnection conn, int id) throws Exception {
        PreparedStatement statement = conn.prepareStatement(
            "INSERT INTO time_zone_test" + suffix + " (i, d) VALUES (?, ?)");
        statement.setInt(1, id);
        statement.setTimestamp(2, new Timestamp(currentTime));
        statement.execute();
    }

    private static void assertDateResult(int suffix, Connection conn, long expectedSecondsEpoch)
        throws Exception
    {
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT i, d as cnt from time_zone_test" + suffix + " order by i")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getDate(2).getTime(), expectedSecondsEpoch * 1000);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getDate(2).getTime(), expectedSecondsEpoch * 1000);
        }
    }

}
