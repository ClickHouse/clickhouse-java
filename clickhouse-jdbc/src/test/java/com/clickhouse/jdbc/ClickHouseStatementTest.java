package com.clickhouse.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseDateTimeValue;

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
            Assert.assertEquals(rs.getTime("t"), Time.valueOf(LocalTime.of(12, 34, 56)));
            Assert.assertEquals(rs.getObject("d"), LocalDate.of(2021, 11, 1));
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
                    "select distinct query from system.query_log where log_comment = 'select something " + uuid + "'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), sql);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testMutation() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties())) {
            ClickHouseStatement stmt = conn.createStatement();
            stmt.execute("drop table if exists test_mutation;"
                    + "create table test_mutation(a String, b UInt32) engine=MergeTree() order by tuple()");
            // [delete from ]tbl a [delete ]where a.b = 1[ settings mutation_async=0]
            // alter table tbl a delete where a.b = 1
            stmt.execute("-- test\nselect 1");
            stmt.execute("-- test\ndelete from test_mutation where b = 1");
            // [update] tbl a [set] a.b = 1 where a.b != 1[ settings mutation_async=0]
            // alter table tbl a update a.b = 1 where a.b != 1
            conn.setClientInfo("ApplicationName", "333");
            conn.createStatement().execute("update test_mutation set b = 22 where b = 1");
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
        try (ClickHouseConnection conn = newConnection(new Properties());
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
    }

    @Test(groups = "integration")
    public void testTimeZone() throws SQLException {
        String dateType = "DateTime32";
        String dateValue = "2020-02-11 00:23:33";
        ClickHouseDateTimeValue v = ClickHouseDateTimeValue.of(dateValue, 0);

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
