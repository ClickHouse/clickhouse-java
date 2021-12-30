package com.clickhouse.jdbc;

import java.io.ByteArrayInputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseBitmap;
import com.clickhouse.client.data.ClickHouseExternalTable;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHousePreparedStatementTest extends JdbcIntegrationTest {
    @DataProvider(name = "typedParameters")
    private Object[][] getTypedParameters() {
        return new Object[][] {
                new Object[] { "Array(DateTime32)", new LocalDateTime[] { LocalDateTime.of(2021, 11, 1, 1, 2, 3),
                        LocalDateTime.of(2021, 11, 2, 2, 3, 4) } } };
    }

    @Test(groups = "integration")
    public void testReadWriteDate() throws SQLException {
        LocalDate d = LocalDate.of(2021, 3, 25);
        Date x = Date.valueOf(d);
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement("insert into test_read_write_date values(?,?,?)")) {
            s.execute("drop table if exists test_read_write_date");
            try {
                s.execute("create table test_read_write_date(id Int32, d1 Date, d2 Date32)engine=Memory");
            } catch (SQLException e) {
                s.execute("create table test_read_write_date(id Int32, d1 Date, d2 Nullable(Date))engine=Memory");
            }
            stmt.setInt(1, 1);
            stmt.setObject(2, d);
            stmt.setObject(3, d);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setDate(2, x);
            stmt.setDate(3, x);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1 });

            ResultSet rs = conn.createStatement().executeQuery("select * from test_read_write_date order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), d);
            Assert.assertEquals(rs.getDate(2), x);
            Assert.assertEquals(rs.getObject(3), d);
            Assert.assertEquals(rs.getDate(3), x);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), d);
            Assert.assertEquals(rs.getDate(2), x);
            Assert.assertEquals(rs.getObject(3), d);
            Assert.assertEquals(rs.getDate(3), x);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDateWithClientTimeZone() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.USE_SERVER_TIME_ZONE_FOR_DATES.getKey(), "false");
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn
                        .prepareStatement("insert into test_read_write_date_cz values (?, ?, ?)")) {
            TimeZone tz = conn.getServerTimeZone();
            // 2021-03-25
            LocalDate d = LocalDateTime.ofInstant(Instant.ofEpochSecond(1616630400L), tz.toZoneId()).toLocalDate();
            Date x = Date.valueOf(d);
            s.execute("drop table if exists test_read_write_date_cz");
            try {
                s.execute("create table test_read_write_date_cz(id Int32, d1 Date, d2 Date32)engine=Memory");
            } catch (SQLException e) {
                s.execute("create table test_read_write_date_cz(id Int32, d1 Date, d2 Nullable(Date))engine=Memory");
            }
            stmt.setInt(1, 1);
            stmt.setObject(2, d);
            stmt.setObject(3, d);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setDate(2, x);
            stmt.setDate(3, x);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1 });

            ResultSet rs = conn.createStatement().executeQuery("select * from test_read_write_date_cz order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), d);
            Assert.assertEquals(rs.getDate(2), x);
            Assert.assertEquals(rs.getObject(3), d);
            Assert.assertEquals(rs.getDate(3), x);
            Assert.assertTrue(rs.next());

            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), d);
            Assert.assertEquals(rs.getDate(2), x);
            Assert.assertEquals(rs.getObject(3), d);
            Assert.assertEquals(rs.getDate(3), x);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDateTime() throws SQLException {
        LocalDateTime dt = LocalDateTime.of(2021, 3, 25, 8, 50, 56);
        Timestamp x = Timestamp.valueOf(dt);
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("insert into test_read_write_datetime values(?,?,?)")) {
            conn.createStatement().execute("drop table if exists test_read_write_datetime;"
                    + "create table test_read_write_datetime(id Int32, d1 DateTime32, d2 DateTime64(3))engine=Memory");
            stmt.setInt(1, 1);
            stmt.setObject(2, dt);
            stmt.setObject(3, dt);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setTimestamp(2, x);
            stmt.setTimestamp(3, x);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1 });

            LocalDateTime dx = dt.atZone(TimeZone.getDefault().toZoneId())
                    .withZoneSameInstant(conn.getServerTimeZone().toZoneId()).toLocalDateTime();
            Timestamp xx = Timestamp.valueOf(dx);
            ResultSet rs = conn.createStatement().executeQuery("select * from test_read_write_datetime order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), dt);
            Assert.assertEquals(rs.getTimestamp(2), x);
            Assert.assertEquals(rs.getObject(3), dt);
            Assert.assertEquals(rs.getTimestamp(3), x);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), dx);
            Assert.assertEquals(rs.getTimestamp(2), xx);
            Assert.assertEquals(rs.getObject(3), dx);
            Assert.assertEquals(rs.getTimestamp(3), xx);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDateTimeWithClientTimeZone() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.USE_SERVER_TIME_ZONE.getKey(), "false");
        LocalDateTime dt = LocalDateTime.of(2021, 3, 25, 8, 50, 56);
        Timestamp x = Timestamp.valueOf(dt);
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_read_write_datetime_cz;"
                    + "create table test_read_write_datetime_cz(id Int32, d1 DateTime32, d2 DateTime64(3))engine=Memory");
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_datetime_cz")) {
                stmt.setInt(1, 1);
                stmt.setObject(2, dt);
                stmt.setObject(3, dt);
                stmt.addBatch();
                stmt.setInt(1, 2);
                stmt.setTimestamp(2, x);
                stmt.setTimestamp(3, x);
                stmt.addBatch();
                int[] results = stmt.executeBatch();
                Assert.assertEquals(results, new int[] { 1, 1 });
            }

            ResultSet rs = s.executeQuery("select * from test_read_write_datetime_cz order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), dt);
            Assert.assertEquals(rs.getTimestamp(2), x);
            Assert.assertEquals(rs.getObject(3), dt);
            Assert.assertEquals(rs.getTimestamp(3), x);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), dt);
            Assert.assertEquals(rs.getTimestamp(2), x);
            Assert.assertEquals(rs.getObject(3), dt);
            Assert.assertEquals(rs.getTimestamp(3), x);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testInsertQueryDateTime64() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement s = conn.createStatement();) {
            s.execute("drop table if exists test_issue_612;"
                    + "CREATE TABLE IF NOT EXISTS test_issue_612 (id UUID, date DateTime64(6)) ENGINE = MergeTree() ORDER BY (id, date)");
            UUID id = UUID.randomUUID();
            long value = 1617359745321000L;
            try (PreparedStatement ps = conn.prepareStatement("insert into test_issue_612 values(?,?)")) {
                ps.setLong(2, value);
                ps.setObject(1, id);
                ps.execute();
                ps.setObject(1, UUID.randomUUID());
                ps.setString(2, "2021-09-01 00:00:00.123456");
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from test_issue_612 where id = ?")) {
                ps.setObject(1, id);
                ResultSet rs = ps.executeQuery();
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getObject(1), id);
                Assert.assertEquals(rs.getObject(2), LocalDateTime.of(2021, 4, 2, 10, 35, 45, 321000000));
                Assert.assertEquals(rs.getLong(2), 1617359745L);
                Assert.assertFalse(rs.next());
            }

            try (PreparedStatement ps = conn.prepareStatement("select * from test_issue_612 where id != ?")) {
                ps.setObject(1, id);
                ResultSet rs = ps.executeQuery();
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getObject(2), LocalDateTime.of(2021, 9, 1, 0, 0, 0, 123456000));
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration")
    public void testBatchInsert() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement("insert into test_batch_insert values(?,?)")) {
            s.execute("drop table if exists test_batch_insert;"
                    + "create table test_batch_insert(id Int32, name Nullable(String))engine=Memory");
            stmt.setInt(1, 1);
            stmt.setString(2, "a");
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setString(2, "b");
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setString(2, null);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1, 1 });

            ResultSet rs = s.executeQuery("select * from test_batch_insert order by id");
            String[] expected = new String[] { "a", "b", null };
            int index = 1;
            while (rs.next()) {
                Assert.assertEquals(rs.getInt(1), index);
                Assert.assertEquals(rs.getString(2), expected[index - 1]);
                index++;
            }
            Assert.assertEquals(index, 4);
        }

        // try with only one column
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement("insert into test_batch_insert(id)")) {
            s.execute("truncate table test_batch_insert");
            stmt.setInt(1, 1);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1, 1 });

            ResultSet rs = s.executeQuery("select * from test_batch_insert order by id");
            int index = 1;
            while (rs.next()) {
                Assert.assertEquals(rs.getInt(1), index);
                Assert.assertEquals(rs.getString(2), null);
                index++;
            }
            Assert.assertEquals(index, 4);
        }

        // now without specifying any column
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement("insert into test_batch_insert")) {
            s.execute("truncate table test_batch_insert");
            stmt.setInt(1, 1);
            stmt.setString(2, "a");
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setString(2, "b");
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setString(2, null);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1, 1 });

            ResultSet rs = s.executeQuery("select * from test_batch_insert order by id");
            String[] expected = new String[] { "a", "b", null };
            int index = 1;
            while (rs.next()) {
                Assert.assertEquals(rs.getInt(1), index);
                Assert.assertEquals(rs.getString(2), expected[index - 1]);
                index++;
            }
            Assert.assertEquals(index, 4);
        }
    }

    @Test(groups = "integration")
    public void testQueryWithDateTime() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        "select id, dt from test_query_datetime where dt > ? order by id")) {
            s.execute("drop table if exists test_query_datetime;"
                    + "create table test_query_datetime(id Int32, dt DateTime32)engine=Memory;"
                    + "insert into test_query_datetime values(1, '2021-03-25 12:34:56'), (2, '2021-03-26 12:34:56')");
            stmt.setObject(1, LocalDateTime.of(2021, 3, 25, 12, 34, 57));
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), LocalDateTime.of(2021, 3, 26, 12, 34, 56));
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testBatchInput() throws SQLException {
        Properties props = new Properties();
        props.setProperty("continueBatchOnError", "true");
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        "insert into test_batch_input select id, name, value from input('id Int32, name Nullable(String), desc Nullable(String), value AggregateFunction(groupBitmap, UInt32)')")) {
            s.execute("drop table if exists test_batch_input;"
                    + "create table test_batch_input(id Int32, name Nullable(String), value AggregateFunction(groupBitmap, UInt32))engine=Memory");
            Object[][] objs = new Object[][] {
                    new Object[] { 1, "a", "aaaaa", ClickHouseBitmap.wrap(1, 2, 3, 4, 5) },
                    new Object[] { 2, "b", null, ClickHouseBitmap.wrap(6, 7, 8, 9, 10) },
                    new Object[] { 3, null, "33333", ClickHouseBitmap.wrap(11, 12, 13) }
            };
            for (Object[] v : objs) {
                stmt.setInt(1, (int) v[0]);
                stmt.setString(2, (String) v[1]);
                stmt.setString(3, (String) v[2]);
                stmt.setObject(4, v[3]);
                stmt.addBatch();
            }
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results.length, objs.length);
            for (int result : results) {
                Assert.assertNotEquals(result, PreparedStatement.EXECUTE_FAILED);
            }

            try (ResultSet rs = s.executeQuery("select * from test_batch_input order by id")) {
                Object[][] values = new Object[objs.length][];
                int index = 0;
                while (rs.next()) {
                    values[index++] = new Object[] {
                            rs.getObject(1), rs.getObject(2), rs.getObject(3)
                    };
                }
                Assert.assertEquals(index, objs.length);
                for (int i = 0; i < objs.length; i++) {
                    Object[] actual = values[i];
                    Object[] expected = objs[i];
                    Assert.assertEquals(actual[0], expected[0]);
                    Assert.assertEquals(actual[1], expected[1]);
                    Assert.assertEquals(actual[2], expected[3]);
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testBatchQuery() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("select * from numbers(100) where number < ?")) {
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(0, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(2, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.addBatch());

            stmt.setInt(1, 3);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testQueryWithExternalTable() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT bitmapContains(my_bitmap, toUInt32(1)) as v1, bitmapContains(my_bitmap, toUInt32(2)) as v2 from {tt 'ext_table'}")) {
            stmt.setObject(1, ClickHouseExternalTable.builder().name("ext_table")
                    .columns("my_bitmap AggregateFunction(groupBitmap,UInt32)").format(ClickHouseFormat.RowBinary)
                    .content(new ByteArrayInputStream(ClickHouseBitmap.wrap(1, 3, 5).toBytes()))
                    .asTempTable()
                    .build());
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getInt(2), 0);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(dataProvider = "typedParameters", groups = "integration")
    public void testArrayParameter(String t, Object v) throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn.prepareStatement("select ?::?")) {
            if (conn.getServerVersion().check("(,21.3]")) {
                return;
            }

            stmt.setObject(1, v);
            // stmt.setString(2, t) or stmt.setObject(2, t) will result in quoted string
            stmt.setObject(2, new StringBuilder(t));
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), v);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testInsertWithFunction() throws Exception {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        "insert into test_issue_315(id, src, dst) values (?,IPv4ToIPv6(toIPv4(?)),IPv4ToIPv6(toIPv4(?)))")) {
            s.execute("drop table if exists test_issue_315;"
                    + "create table test_issue_315(id Int32, src IPv6, dst IPv6)engine=Memory");

            stmt.setObject(1, 1);
            stmt.setString(2, "127.0.0.1");
            stmt.setString(3, "127.0.0.2");
            Assert.assertEquals(stmt.executeUpdate(), 1);

            // omitted '(id, src, dst)' in the query for simplicity
            try (PreparedStatement ps = conn.prepareStatement("insert into test_issue_315")) {
                stmt.setObject(1, 2);
                stmt.setObject(2, Inet4Address.getByName("127.0.0.2"));
                stmt.setObject(3, Inet6Address.getByName("::1"));
                Assert.assertEquals(stmt.executeUpdate(), 1);
            }

            ResultSet rs = s.executeQuery("select * from test_issue_315 order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getString(2), "0:0:0:0:0:ffff:7f00:1");
            Assert.assertEquals(rs.getString(3), "0:0:0:0:0:ffff:7f00:2");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getString(2), "0:0:0:0:0:ffff:7f00:2");
            Assert.assertEquals(rs.getString(3), "0:0:0:0:0:ffff:0:0");
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testQueryWithNamedParameter() throws SQLException {
        Properties props = new Properties();
        props.setProperty(JdbcConfig.PROP_NAMED_PARAM, "true");
        LocalDateTime ts = LocalDateTime.ofEpochSecond(10000, 123456789, ZoneOffset.UTC);
        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement stmt = conn
                        .prepareStatement("select :ts1 ts1, :ts2(DateTime32) ts2, :ts2 ts3")) {
            // just two parameters here - ts2 is referenced twice
            stmt.setObject(1, ts);
            stmt.setObject(2, ts);
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "1970-01-01 02:46:40.123456789");
            Assert.assertEquals(rs.getString(2), "1970-01-01 02:46:40");
            Assert.assertEquals(rs.getString(3), "1970-01-01 02:46:40");
            Assert.assertFalse(rs.next());
        }

        // try again using JDBC standard question mark placeholder
        try (ClickHouseConnection conn = newConnection();
                PreparedStatement stmt = conn
                        .prepareStatement("select ? ts1, ? ts2, ? ts3")) {
            // unlike above, this time we have 3 parameters
            stmt.setObject(1, "1970-01-01 02:46:40.123456789");
            stmt.setObject(2, "1970-01-01 02:46:40");
            stmt.setObject(3, "1970-01-01 02:46:40");
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "1970-01-01 02:46:40.123456789");
            Assert.assertEquals(rs.getString(2), "1970-01-01 02:46:40");
            Assert.assertEquals(rs.getString(3), "1970-01-01 02:46:40");
            Assert.assertFalse(rs.next());
        }
    }
}
