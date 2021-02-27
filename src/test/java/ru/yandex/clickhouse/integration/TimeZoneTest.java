package ru.yandex.clickhouse.integration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseContainerForTest;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.util.ClickHouseRowBinaryStream;
import ru.yandex.clickhouse.util.ClickHouseStreamCallback;

import static org.testng.Assert.fail;

public class TimeZoneTest {

    private ClickHouseConnection connectionServerTz;
    private ClickHouseConnection connectionManualTz;

    private long currentTime = 1000 * (System.currentTimeMillis() / 1000);

    @BeforeTest
    public void setUp() throws Exception {
        ClickHouseDataSource datasourceServerTz = ClickHouseContainerForTest.newDataSource();;
        connectionServerTz = datasourceServerTz.getConnection();
        TimeZone serverTimeZone = connectionServerTz.getTimeZone();
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUseServerTimeZone(false);
        int serverTimeZoneOffsetHours = (int) TimeUnit.MILLISECONDS.toHours(serverTimeZone.getOffset(currentTime));
        int manualTimeZoneOffsetHours = serverTimeZoneOffsetHours - 1;
        properties.setUseTimeZone("GMT" + (manualTimeZoneOffsetHours > 0 ? "+" : "")  + manualTimeZoneOffsetHours + ":00");
        ClickHouseDataSource dataSourceManualTz = ClickHouseContainerForTest.newDataSource(properties);
        connectionManualTz = dataSourceManualTz.getConnection();
        connectionServerTz.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        if (connectionServerTz != null) {
            connectionServerTz.close();
            connectionServerTz = null;
        }
        if (connectionManualTz != null) {
            connectionManualTz.close();
            connectionManualTz = null;
        }
    }

    @Test
    public void timeZoneTestTimestamp() throws Exception {

        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.time_zone_test");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.time_zone_test (i Int32, d DateTime) ENGINE = TinyLog"
        );

        PreparedStatement statement = connectionServerTz.prepareStatement("INSERT INTO test.time_zone_test (i, d) VALUES (?, ?)");
        statement.setInt(1, 1);
        statement.setTimestamp(2, new Timestamp(currentTime));
        statement.execute();

        PreparedStatement statementUtc = connectionManualTz.prepareStatement("INSERT INTO test.time_zone_test (i, d) VALUES (?, ?)");
        statementUtc.setInt(1, 2);
        statementUtc.setTimestamp(2, new Timestamp(currentTime));
        statementUtc.execute();

        ResultSet rs = connectionServerTz.createStatement().executeQuery("SELECT i, d as cnt from test.time_zone_test order by i");
        // server write, server read
        rs.next();
        Assert.assertEquals(rs.getTimestamp(2).getTime(), currentTime);

        // manual write, server read
        rs.next();
        Assert.assertEquals(rs.getTimestamp(2).getTime(), currentTime - TimeUnit.HOURS.toMillis(1));

        ResultSet rsMan = connectionManualTz.createStatement().executeQuery("SELECT i, d as cnt from test.time_zone_test order by i");
        // server write, manual read
        rsMan.next();
        Assert.assertEquals(rsMan.getTimestamp(2).getTime(), currentTime + TimeUnit.HOURS.toMillis(1));
        // manual write, manual read
        rsMan.next();
        Assert.assertEquals(rsMan.getTimestamp(2).getTime(), currentTime);
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

    @Test
    public void testTimeZoneParseSQLDate1() throws Exception {
        try {
            ClickHouseDataSource ds = createDataSource(false, null, false);
            ds.getConnection();
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testTimeZoneParseSQLDate2() throws Exception {
        int suffix = 2;
        Long offset = Long.valueOf(
            TimeUnit.MILLISECONDS.toHours(
                connectionServerTz.getTimeZone().getOffset(currentTime)));
        ClickHouseDataSource ds = createDataSource(false, offset, false);
        resetDateTestTable(suffix);
        ClickHouseConnection conn = ds.getConnection();
        insertDateTestData(suffix, connectionServerTz, 1);
        insertDateTestData(suffix, conn, 1);

        // TODO revisit this before 0.3.0 release
        currentTime = Instant.ofEpochMilli(currentTime)
            .atZone(ZoneId.systemDefault())
            .truncatedTo(ChronoUnit.DAYS)
            .toEpochSecond() * 1000L;

        assertDateResult(suffix, conn, Instant.ofEpochMilli(currentTime)
            .atZone(ZoneId.systemDefault())
            .truncatedTo(ChronoUnit.DAYS)
            .toEpochSecond());
        conn.close();
        ds = null;
    }

    @Test
    public void testTimeZoneParseSQLDate3() throws Exception {
        try {
            ClickHouseDataSource ds = createDataSource(false, null, false);
            ds.getConnection();
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testTimeZoneParseSQLDate4() throws Exception {
        int suffix = 4;
        Long offset = Long.valueOf(
            TimeUnit.MILLISECONDS.toHours(
                connectionServerTz.getTimeZone().getOffset(currentTime)));
        ClickHouseDataSource ds = createDataSource(false, offset, true);
        resetDateTestTable(suffix);
        ClickHouseConnection conn = ds.getConnection();
        insertDateTestData(suffix, connectionServerTz, 1);
        insertDateTestData(suffix, conn, 1);
        assertDateResult(
            suffix, conn,
            Instant.ofEpochMilli(currentTime)
                .atOffset(ZoneOffset.ofHours(offset.intValue()))
                .truncatedTo(ChronoUnit.DAYS)
                .toEpochSecond());
        conn.close();
        ds = null;
    }

    // TODO revisit this before 0.3.0 release
    @Test(enabled = false)
    public void testTimeZoneParseSQLDate5() throws Exception {
        int suffix = 5;
        ClickHouseDataSource ds = createDataSource(true, null, false);
        resetDateTestTable(suffix);
        ClickHouseConnection conn = ds.getConnection();
        insertDateTestData(suffix, connectionServerTz, 1);
        insertDateTestData(suffix, conn, 1);
        
        currentTime -= currentTime % (24 * 3600 * 1000L);
        currentTime -= ZoneId.systemDefault().getRules().getOffset(
            Instant.ofEpochMilli(currentTime)).getTotalSeconds() * 1000L;
    
        assertDateResult(
            suffix, conn,
            Instant.ofEpochMilli(currentTime)
                .atZone(ZoneId.systemDefault())
                .truncatedTo(ChronoUnit.DAYS)
                .toEpochSecond());
        conn.close();
        ds = null;
    }

    @Test
    public void testTimeZoneParseSQLDate6() throws Exception {
        try {
            ClickHouseDataSource ds = createDataSource(true, Long.valueOf(1L), false);
            ds.getConnection();
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testTimeZoneParseSQLDate7() throws Exception {
        int suffix = 7;
        ClickHouseDataSource ds = createDataSource(true, null, true);
        resetDateTestTable(suffix);
        ClickHouseConnection conn = ds.getConnection();
        insertDateTestData(suffix, connectionServerTz, 1);
        insertDateTestData(suffix, conn, 1);
        assertDateResult(
            suffix, conn,
            Instant.ofEpochMilli(currentTime)
                .atZone(connectionServerTz.getTimeZone().toZoneId())
                .truncatedTo(ChronoUnit.DAYS)
                .toEpochSecond());
        conn.close();
        ds = null;
    }

    @Test
    public void testTimeZoneParseSQLDate8() throws Exception {
        try {
            ClickHouseDataSource ds = createDataSource(true, Long.valueOf(1L), true);
            ds.getConnection();
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
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

    @Test
    public void dateInsertTest() throws SQLException {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.date_insert");
        connectionServerTz.createStatement().execute(
                "CREATE TABLE test.date_insert (" +
                        "i UInt8," +
                        "d Date" +
                        ") ENGINE = TinyLog"
        );


        final Date date = new Date(currentTime);
        Date localStartOfDay = withTimeAtStartOfDay(date);

        connectionServerTz.createStatement().sendRowBinaryStream(
                "INSERT INTO test.date_insert (i, d)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                            stream.writeUInt8(1);
                            stream.writeDate(date);
                    }
                }
        );

        connectionManualTz.createStatement().sendRowBinaryStream(
                "INSERT INTO test.date_insert (i, d)",
                new ClickHouseStreamCallback() {
                    @Override
                    public void writeTo(ClickHouseRowBinaryStream stream) throws IOException {
                        stream.writeUInt8(2);
                        stream.writeDate(date);
                    }
                }
        );

        ResultSet rsMan = connectionManualTz.createStatement().executeQuery("select d from test.date_insert order by i");
        ResultSet rsSrv = connectionServerTz.createStatement().executeQuery("select d from test.date_insert order by i");
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

    @Test
    public void testParseColumnsWithDifferentTimeZones() throws Exception {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.fun_with_timezones");
        connectionServerTz.createStatement().execute(
            "CREATE TABLE test.fun_with_timezones (" +
                "i         UInt8," +
                "dt_server DateTime," +
                "dt_berlin DateTime('Europe/Berlin')," +
                "dt_lax    DateTime('America/Los_Angeles')" +
            ") ENGINE = TinyLog"
        );
        connectionServerTz.createStatement().execute(
            "INSERT INTO test.fun_with_timezones (i, dt_server, dt_berlin, dt_lax) " +
            "VALUES (42, 1557136800, 1557136800, 1557136800)");
        ResultSet rs_timestamps = connectionServerTz.createStatement().executeQuery(
            "SELECT i, toUnixTimestamp(dt_server), toUnixTimestamp(dt_berlin), toUnixTimestamp(dt_lax) " +
            "FROM test.fun_with_timezones");
        rs_timestamps.next();
        Assert.assertEquals(rs_timestamps.getLong(2), 1557136800);
        Assert.assertEquals(rs_timestamps.getLong(3), 1557136800);
        Assert.assertEquals(rs_timestamps.getLong(4), 1557136800);

        ResultSet rs_datetimes = connectionServerTz.createStatement().executeQuery(
            "SELECT i, dt_server, dt_berlin, dt_lax " +
            "FROM test.fun_with_timezones");
        rs_datetimes.next();
        Assert.assertEquals(rs_datetimes.getTimestamp(2).getTime(), 1557136800000L);
        Assert.assertEquals(rs_datetimes.getTimestamp(3).getTime(), 1557136800000L);
        Assert.assertEquals(rs_datetimes.getTimestamp(4).getTime(), 1557136800000L);
    }

    @Test
    public void testParseColumnsWithDifferentTimeZonesArray() throws Exception {
        connectionServerTz.createStatement().execute("DROP TABLE IF EXISTS test.fun_with_timezones_array");
        connectionServerTz.createStatement().execute(
            "CREATE TABLE test.fun_with_timezones_array (" +
                "i         UInt8," +
                "dt_server Array(DateTime)," +
                "dt_berlin Array(DateTime('Europe/Berlin'))," +
                "dt_lax    Array(DateTime('America/Los_Angeles'))" +
            ") ENGINE = TinyLog"
        );
        connectionServerTz.createStatement().execute(
            "INSERT INTO test.fun_with_timezones_array (i, dt_server, dt_berlin, dt_lax) " +
            "VALUES (42, [1557136800], [1557136800], [1557136800])");
        ResultSet rs_timestamps = connectionServerTz.createStatement().executeQuery(
            "SELECT i, array(toUnixTimestamp(dt_server[1])), array(toUnixTimestamp(dt_berlin[1])), array(toUnixTimestamp(dt_lax[1])) " +
            "FROM test.fun_with_timezones_array");
        rs_timestamps.next();
        Assert.assertEquals(((long[]) rs_timestamps.getArray(2).getArray())[0], 1557136800);
        Assert.assertEquals(((long[]) rs_timestamps.getArray(3).getArray())[0], 1557136800);
        Assert.assertEquals(((long[]) rs_timestamps.getArray(4).getArray())[0], 1557136800);

        ResultSet rs_datetimes = connectionServerTz.createStatement().executeQuery(
            "SELECT i, dt_server, dt_berlin, dt_lax " +
            "FROM test.fun_with_timezones_array");
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

    private static ClickHouseDataSource createDataSource(boolean useServerTimeZone,
        Long manualTimeZoneOffsetHours, boolean useServerTimeZoneForParsingDates)
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
        return ClickHouseContainerForTest.newDataSource(props);
    }

    private void resetDateTestTable(int suffix) throws Exception {
        connectionServerTz.createStatement().execute(
            "DROP TABLE IF EXISTS test.time_zone_test" + suffix);
        connectionServerTz.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test.time_zone_test" + suffix + " (i Int32, d DateTime) ENGINE = TinyLog"
        );
    }

    private void insertDateTestData(int suffix, ClickHouseConnection conn, int id) throws Exception {
        PreparedStatement statement = conn.prepareStatement(
            "INSERT INTO test.time_zone_test" + suffix + " (i, d) VALUES (?, ?)");
        statement.setInt(1, id);
        statement.setTimestamp(2, new Timestamp(currentTime));
        statement.execute();
    }

    private static void assertDateResult(int suffix, Connection conn, long expectedSecondsEpoch)
        throws Exception
    {
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT i, d as cnt from test.time_zone_test" + suffix + " order by i");
        rs.next();
        Assert.assertEquals(rs.getDate(2).getTime(), expectedSecondsEpoch * 1000);
        rs.next();
        Assert.assertEquals(rs.getDate(2).getTime(), expectedSecondsEpoch * 1000);
    }

}
