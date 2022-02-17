package com.clickhouse.jdbc;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.http.config.ClickHouseHttpOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseStatementTest extends JdbcIntegrationTest {
    @Test(groups = "integration")
    public void testJdbcEscapeSyntax() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "select * from (select {d '2021-11-01'} as D, {t '12:34:56'} as T, "
                            + "{ts '2021-11-01 12:34:56'} as TS) as {tt 'temp_table'}");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject("ts", LocalDateTime.class), LocalDateTime.of(2021, 11, 1, 12, 34, 56));
            Assert.assertEquals(rs.getObject("t", LocalTime.class), LocalTime.of(12, 34, 56));
            Assert.assertEquals(rs.getObject("d"), LocalDate.of(2021, 11, 1));
            Assert.assertEquals(rs.getTime("t"), Time.valueOf(LocalTime.of(12, 34, 56)));
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testSwitchSchema() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", "system");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            String dbName = "test_switch_schema";
            stmt.execute(
                    ClickHouseParameterizedQuery.apply("drop database if exists :db; "
                            + "create database :db; "
                            + "create table :db.:db (a Int32) engine=Memory",
                            Collections.singletonMap("db", dbName)));
            ResultSet rs = stmt.executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertFalse(rs.next());
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select * from test_switch_schema"));
            conn.setSchema(dbName);
            rs = stmt.executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), dbName);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "local")
    public void testLogComment() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.LOG_LEADING_COMMENT.getKey(), "true");
        try (ClickHouseConnection conn = newConnection(props)) {
            ClickHouseStatement stmt = conn.createStatement();
            String uuid = UUID.randomUUID().toString();
            String sql = "-- select something " + uuid + "\nselect 12345";
            stmt.execute(sql + "; system flush logs;");
            ResultSet rs = stmt.executeQuery(
                    "select distinct query, type from system.query_log where log_comment = 'select something " + uuid
                            + "'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), sql);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testMutation() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props); ClickHouseStatement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute("drop table if exists test_mutation;"
                    + "create table test_mutation(a String, b UInt32) engine=MergeTree() order by tuple()"),
                    "Should not return result set");
            // [delete from ]tbl a [delete ]where a.b = 1[ settings mutation_async=0]
            // alter table tbl a delete where a.b = 1
            Assert.assertTrue(stmt.execute("-- test\nselect 1"), "Should return a result set");
            Assert.assertFalse(stmt.execute("-- test\ndelete from test_mutation where b = 1"),
                    "Should not return result set");
            // [update] tbl a [set] a.b = 1 where a.b != 1[ settings mutation_async=0]
            // alter table tbl a update a.b = 1 where a.b != 1
            conn.setClientInfo("ApplicationName", "333");
            Assert.assertEquals(conn.createStatement().executeUpdate("update test_mutation set b = 22 where b = 1"), 0);

            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeUpdate("update non_exist_table set value=1 where key=1"));

            stmt.addBatch("select 1");
            stmt.addBatch("select * from non_exist_table");
            stmt.addBatch("select 2");
            Assert.assertThrows(SQLException.class, () -> stmt.executeBatch());
        }

        props.setProperty(JdbcConfig.PROP_CONTINUE_BATCH, "true");
        try (ClickHouseConnection conn = newConnection(props); ClickHouseStatement stmt = conn.createStatement()) {
            stmt.addBatch("select 1");
            stmt.addBatch("select * from non_exist_table");
            stmt.addBatch("insert into test_mutation values('a',1)");
            stmt.addBatch("select 2");
            Assert.assertEquals(stmt.executeBatch(), new int[] { 0, ClickHouseStatement.EXECUTE_FAILED, 1, 0 });
        }
    }

    @Test(groups = "integration")
    public void testAsyncInsert() throws SQLException {
        if (DEFAULT_PROTOCOL != ClickHouseProtocol.HTTP) {
            return;
        }

        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props)) {
            if (conn.getServerVersion().check("(,21.12)")) {
                return;
            }
        }

        props.setProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), "async_insert=1,wait_for_async_insert=1");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("drop table if exists test_async_insert; "
                    + "create table test_async_insert(id UInt32, s String) ENGINE = Memory; "
                    + "INSERT INTO test_async_insert VALUES(1, 'a'); "
                    + "select * from test_async_insert");
            ResultSet rs = stmt.getResultSet();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getString(2), "a");
            Assert.assertFalse(rs.next());
        }

        props.setProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), "async_insert=1,wait_for_async_insert=0");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("truncate table test_async_insert; "
                    + "INSERT INTO test_async_insert VALUES(1, 'a'); "
                    + "select * from test_async_insert");
            ResultSet rs = stmt.getResultSet();
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testCancelQuery() throws Exception {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement stmt = conn.createStatement();) {
            CountDownLatch c = new CountDownLatch(1);
            ClickHouseClient.submit(() -> stmt.executeQuery("select * from numbers(100000000)")).whenComplete(
                    (rs, e) -> {
                        int index = 0;

                        try {
                            while (rs.next()) {
                                if (index++ < 1) {
                                    c.countDown();
                                }
                            }
                        } catch (SQLException ex) {
                            // ignore
                        }
                    });
            try {
                c.await(5, TimeUnit.SECONDS);
            } finally {
                stmt.cancel();
            }
        }
    }

    @Test(groups = "integration")
    public void testSimpleAggregateFunction() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("drop table if exists test_simple_agg_func; "
                    + "CREATE TABLE test_simple_agg_func (x SimpleAggregateFunction(max, UInt64)) ENGINE=AggregatingMergeTree ORDER BY tuple(); "
                    + "INSERT INTO test_simple_agg_func VALUES(1)");

            ResultSet rs = stmt.executeQuery("select * from test_simple_agg_func");
            // sorry, not supported at this point
            Assert.assertThrows(IllegalArgumentException.class, () -> rs.next());
        }
    }

    @Test(groups = "integration")
    public void testWrapperObject() throws SQLException {
        String sql = "SELECT CAST('[(''a'',''b'')]' AS Array(Tuple(String, String))), ('a', 'b')";
        List<?> expectedTuple = Arrays.asList("a", "b");
        Object expectedArray = new List[] { expectedTuple };
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getArray(1).getArray(), expectedArray);
            Assert.assertEquals(rs.getObject(1), expectedArray);
            Assert.assertEquals(rs.getObject(2), expectedTuple);
            Assert.assertFalse(rs.next());
        }

        props.setProperty(JdbcConfig.PROP_WRAPPER_OBJ, "true");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getArray(1).getArray(), expectedArray);
            Assert.assertEquals(((Array) rs.getObject(1)).getArray(), expectedArray);
            Assert.assertEquals(((Struct) rs.getObject(2)).getAttributes(), expectedTuple.toArray(new String[0]));
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testQuery() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            ClickHouseStatement stmt = conn.createStatement();
            stmt.setMaxRows(10);
            ResultSet rs = stmt.executeQuery("select * from numbers(100)");

            try (ResultSet colRs = conn.getMetaData().getColumns(null, "system", "query_log", "")) {
                while (colRs.next()) {
                    continue;
                }
            }

            while (rs.next()) {
                continue;
            }

            // batch query
            stmt.addBatch("select 1");
            stmt.addBatch("select 2");
            stmt.addBatch("select 3");
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testMultiStatementQuery() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            ClickHouseStatement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("set join_use_nulls=1;\n"
                    + "select a.k, b.m from ( "
                    + "    select 1 k, null v union all select 2 k, 'a' v "
                    + ") a left outer join ( select 1 f, 2 m ) b on a.k = b.f "
                    + "order by a.k");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getInt(2), 2);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getInt(2), 0);
            Assert.assertEquals(rs.getObject(2), null);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testTimestamp() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select now(), now('Asia/Chongqing')");
            Assert.assertTrue(rs.next());
            LocalDateTime dt1 = (LocalDateTime) rs.getObject(1);
            LocalDateTime dt2 = rs.getObject(1, LocalDateTime.class);
            Assert.assertTrue(dt1 == dt2);
            OffsetDateTime ot1 = (OffsetDateTime) rs.getObject(2);
            OffsetDateTime ot2 = rs.getObject(2, OffsetDateTime.class);
            Assert.assertTrue(ot1 == ot2);
            Assert.assertFalse(rs.next());
        }

        String tz = "America/Los_Angeles";
        String sql = "SELECT toDateTime(1616633456), toDateTime(1616633456, 'Etc/UTC'), "
                + "toDateTime(1616633456, 'America/Los_Angeles'),  toDateTime(1616633456, 'Asia/Chongqing'), "
                + "toDateTime(1616633456, 'Europe/Berlin'), toUInt32(toDateTime('2021-03-25 08:50:56')), "
                + "toUInt32(toDateTime('2021-03-25 08:50:56', 'Asia/Chongqing'))";
        props.setProperty("use_time_zone", tz);
        props.setProperty("use_server_time_zone", "false");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1),
                    ZonedDateTime.ofInstant(Instant.ofEpochSecond(1616633456L), ZoneId.of(tz))
                            .toLocalDateTime());
            Assert.assertFalse(rs.next());
        }
    }

    // @Test(groups = "integration")
    // public void testAggregateFunction() throws SQLException {
    // Properties props = new Properties();
    // try (ClickHouseConnection conn = newConnection(props);
    // ClickHouseStatement stmt = conn.createStatement()) {
    // ResultSet rs = stmt.executeQuery("select anyState(n) from (select
    // toInt32(number + 5) n from numbers(3))");
    // Assert.assertTrue(rs.next());
    // Assert.assertEquals(rs.getObject(1), 5);
    // Assert.assertFalse(rs.next());

    // rs = stmt.executeQuery("select anyState(null)");
    // Assert.assertTrue(rs.next());
    // Assert.assertNull(rs.getObject(1));
    // Assert.assertFalse(rs.next());

    // rs = stmt.executeQuery("select anyState(n) from (select toString(number) n
    // from numbers(0))");
    // Assert.assertTrue(rs.next());
    // Assert.assertNull(rs.getObject(1));
    // Assert.assertFalse(rs.next());
    // }
    // }

    @Test(groups = "integration")
    public void testCustomTypeMappings() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select cast('a' as Enum('a'=1,'b'=2))");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), "a");
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next());
        }

        props.setProperty("typeMappings",
                "Enum8=java.lang.Byte,DateTime64=java.lang.String, String=com.clickhouse.client.ClickHouseDataType");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "select cast('a' as Enum('a'=1,'b'=2)), toDateTime64('2021-12-21 12:34:56.789',3), 'Float64'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), (byte) 1);
            Assert.assertEquals(rs.getObject(2), "2021-12-21 12:34:56.789");
            Assert.assertEquals(rs.getObject(3), ClickHouseDataType.Float64);
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testNestedDataTypes() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select (1,2) as t, [3,4] as a");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), Arrays.asList((short) 1, (short) 2));
            Assert.assertEquals(rs.getObject(2), new short[] { (short) 3, (short) 4 });
            Assert.assertFalse(rs.next());
        }

        props.setProperty("wrapperObject", "true");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select (1,2) as t, [3,4] as a");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(((ClickHouseStruct) rs.getObject(1)).getAttributes(),
                    new Object[] { (short) 1, (short) 2 });
            Assert.assertEquals(((ClickHouseArray) rs.getObject(2)).getArray(), rs.getArray(2).getArray());
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testTimeZone() throws SQLException {
        String dateType = "DateTime32";
        String dateValue = "2020-02-11 00:23:33";
        ClickHouseDateTimeValue v = ClickHouseDateTimeValue.of(dateValue, 0, ClickHouseValues.UTC_TIMEZONE);

        Properties props = new Properties();
        String[] timeZones = new String[] { "Asia/Chongqing", "America/Los_Angeles", "Europe/Moscow", "Etc/UTC",
                "Europe/Berlin" };
        StringBuilder columns = new StringBuilder().append("d0 ").append(dateType);
        StringBuilder constants = new StringBuilder().append(ClickHouseValues.convertToQuotedString(dateValue));
        StringBuilder currents = new StringBuilder().append("now()");
        StringBuilder parameters = new StringBuilder().append("?,?");
        int len = timeZones.length;
        Calendar[] calendars = new Calendar[len + 1];
        for (int i = 0; i < len; i++) {
            String timeZoneId = timeZones[i];
            columns.append(",d").append(i + 1).append(' ').append(dateType).append("('").append(timeZoneId)
                    .append("')");
            constants.append(',').append(ClickHouseValues.convertToQuotedString(dateValue));
            currents.append(",now()");
            parameters.append(",?");
            calendars[i] = new GregorianCalendar(TimeZone.getTimeZone(timeZoneId));
        }
        len++;
        try (ClickHouseConnection conn = newConnection(props);
                Connection mconn = newMySqlConnection(props);
                Statement mstmt = mconn.createStatement();) {
            ClickHouseStatement stmt = conn.createStatement();
            stmt.execute("drop table if exists test_tz;" + "create table test_tz(no String," + columns.toString()
                    + ") engine=Memory;" + "insert into test_tz values('0 - Constant'," + constants.toString() + ");"
                    + "insert into test_tz values('1 - Current'," + currents.toString() + ");");

            String sql = "insert into test_tz values(" + parameters.toString() + ")";
            try (PreparedStatement ps = conn.prepareStatement(sql);
                    PreparedStatement mps = mconn.prepareStatement(sql)) {
                int index = 2;
                mps.setString(1, (0 - index) + " - String");
                ps.setString(1, index++ + " - String");
                for (int i = 1; i <= len; i++) {
                    ps.setString(i + 1, v.asString());
                    mps.setString(i + 1, v.asString());
                }
                ps.addBatch();
                mps.addBatch();

                ps.setString(1, index++ + " - LocalDateTime");
                for (int i = 1; i <= len; i++) {
                    ps.setObject(i + 1, v.asDateTime());
                }
                ps.addBatch();

                ps.setString(1, index++ + " - OffsetDateTime");
                for (int i = 1; i <= len; i++) {
                    ps.setObject(i + 1, v.asOffsetDateTime());
                }
                ps.addBatch();

                ps.setString(1, index++ + " - DateTime");
                for (int i = 1; i <= len; i++) {
                    if (i == 1) {
                        ps.setObject(i + 1, v.asDateTime());
                    } else {
                        ps.setObject(i + 1, v.asDateTime().atZone(TimeZone.getTimeZone(timeZones[i - 2]).toZoneId())
                                .toOffsetDateTime());
                    }
                }
                ps.addBatch();

                mps.setString(1, (0 - index) + " - BigDecimal");
                ps.setString(1, index++ + " - BigDecimal");
                for (int i = 1; i <= len; i++) {
                    ps.setBigDecimal(i + 1, v.asBigDecimal());
                    mps.setBigDecimal(i + 1, v.asBigDecimal());
                }
                ps.addBatch();
                mps.addBatch();

                mps.setString(1, (0 - index) + " - Timestamp");
                ps.setString(1, index++ + " - Timestamp");
                for (int i = 1; i <= len; i++) {
                    ps.setTimestamp(i + 1, Timestamp.valueOf(v.asDateTime()));
                    mps.setTimestamp(i + 1, Timestamp.valueOf(v.asDateTime()));
                }
                ps.addBatch();
                mps.addBatch();

                for (int j = 0; j < len; j++) {
                    Calendar c = calendars[j];
                    mps.setString(1, (0 - index) + " - Timestamp(" + (c == null ? "" : c.getTimeZone().getID()) + ")");
                    ps.setString(1, index++ + " - Timestamp(" + (c == null ? "" : c.getTimeZone().getID()) + ")");
                    for (int i = 1; i <= len; i++) {
                        ps.setTimestamp(i + 1, Timestamp.valueOf(v.asDateTime()), c);
                        mps.setTimestamp(i + 1, Timestamp.valueOf(v.asDateTime()), c);
                    }
                    ps.addBatch();
                    mps.addBatch();
                }

                int[] results = ps.executeBatch();
                mps.executeBatch();
            }

            try (ResultSet rs = stmt
                    .executeQuery("select * from test_tz order by toInt32(splitByString(' - ', no)[1])");
                    ResultSet mrs = mstmt
                            .executeQuery("select * from test_tz order by toInt32(splitByString(' - ', no)[1])")) {
                int row = 0;
                while (rs.next()) {
                    row++;
                    Assert.assertTrue(mrs.next());

                    for (int i = 1; i <= len; i++) {
                        String msg = String.format(Locale.ROOT, "row: %d, column: %d", row, i + 1);
                        // Assert.assertEquals(rs.getObject(i + 1), mrs.getObject(i + 1));
                        Assert.assertEquals(rs.getDate(i + 1), mrs.getDate(i + 1), msg);
                        Assert.assertEquals(rs.getString(i + 1), mrs.getString(i + 1), msg);
                        Assert.assertEquals(rs.getTimestamp(i + 1), mrs.getTimestamp(i + 1), msg);
                        Assert.assertEquals(rs.getTime(i + 1), mrs.getTime(i + 1), msg);
                        for (int j = 0; j < len; j++) {
                            msg = String.format(Locale.ROOT, "row: %d, column: %d, calendar: %s", row, i + 1,
                                    calendars[j]);
                            Assert.assertEquals(rs.getTimestamp(i + 1, calendars[j]),
                                    mrs.getTimestamp(i + 1, calendars[j]), msg);
                            Assert.assertEquals(rs.getTime(i + 1, calendars[j]), mrs.getTime(i + 1, calendars[j]), msg);
                        }
                    }
                }
            }
        }
    }
}
