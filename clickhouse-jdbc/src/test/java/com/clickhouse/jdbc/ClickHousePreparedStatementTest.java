package com.clickhouse.jdbc;

import java.io.ByteArrayInputStream;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataStreamFactory;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHousePipedOutputStream;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.data.ClickHouseBitmap;
import com.clickhouse.client.data.ClickHouseExternalTable;
import com.clickhouse.jdbc.internal.InputBasedPreparedStatement;
import com.clickhouse.jdbc.internal.SqlBasedPreparedStatement;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHousePreparedStatementTest extends JdbcIntegrationTest {
    @DataProvider(name = "columnsWithDefaultValue")
    private Object[][] getColumnsWithDefaultValue() {
        return new Object[][] {
                new Object[] { "Bool", "false", "false" },
                new Object[] { "Date", "1", "1970-01-02" },
                new Object[] { "Date32", "-1", "1969-12-31" },
                new Object[] { "DateTime32('UTC')", "'1970-01-01 00:00:01'", "1970-01-01 00:00:01" },
                new Object[] { "DateTime64(3, 'UTC')", "'1970-01-01 00:00:01.234'", "1970-01-01 00:00:01.234" },
                new Object[] { "Decimal(10,4)", "10.1234", "10.1234" },
                new Object[] { "Enum8('x'=5,'y'=6)", "'y'", "y" },
                new Object[] { "Enum8('x'=5,'y'=6)", "5", "x" },
                new Object[] { "Enum16('xx'=55,'yy'=66)", "'yy'", "yy" },
                new Object[] { "Enum16('xx'=55,'yy'=66)", "55", "xx" },
                new Object[] { "Float32", "3.2", "3.2" },
                new Object[] { "Float64", "6.4", "6.4" },
                new Object[] { "Int8", "-1", "-1" },
                new Object[] { "UInt8", "1", "1" },
                new Object[] { "Int16", "-3", "-3" },
                new Object[] { "UInt16", "3", "3" },
                new Object[] { "Int32", "-5", "-5" },
                new Object[] { "UInt32", "7", "7" },
                new Object[] { "Int64", "-9", "-9" },
                new Object[] { "UInt64", "11", "11" },
                new Object[] { "Int128", "-13", "-13" },
                new Object[] { "UInt128", "17", "17" },
                new Object[] { "Int256", "-19", "-19" },
                new Object[] { "UInt256", "23", "23" },
        };
    }

    @DataProvider(name = "nonBatchQueries")
    private Object[][] getNonBatchQueries() {
        return new Object[][] {
                new Object[] { "input", "insert into %s" },
                new Object[] { "sql", "insert into %s values(?+0, ?)" },
                new Object[] { "table", "insert into %s select * from {tt 'tbl'}" }
        };
    }

    @DataProvider(name = "typedParameters")
    private Object[][] getTypedParameters() {
        return new Object[][] {
                new Object[] { "Array(DateTime32)", new LocalDateTime[] { LocalDateTime.of(2021, 11, 1, 1, 2, 3),
                        LocalDateTime.of(2021, 11, 2, 2, 3, 4) } } };
    }

    @DataProvider(name = "statementAndParams")
    private Object[][] getStatementAndParameters() {
        return new Object[][] {
                // ddl
                new Object[] { "ddl", "drop table if exists non_existing_table", SqlBasedPreparedStatement.class, false,
                        null, false },
                // query
                new Object[] { "select1", "select 1", SqlBasedPreparedStatement.class, true, null, false },
                new Object[] { "select_param", "select ?", SqlBasedPreparedStatement.class, true, new String[] { "1" },
                        false },
                // mutation
                new Object[] { "insert_static", "insert into $table values(1)",
                        SqlBasedPreparedStatement.class, false, null,
                        false },
                new Object[] { "insert_table", "insert into $table", InputBasedPreparedStatement.class, false,
                        new String[] { "2" }, true },
                new Object[] { "insert_param", "insert into $table values(?)", InputBasedPreparedStatement.class,
                        false, new String[] { "3" }, true },
                new Object[] { "insert_param", "insert into $table values(trim(?))",
                        SqlBasedPreparedStatement.class, false, new String[] { "4" }, true },
                new Object[] { "insert_input", "insert into $table select s from input('s String')",
                        InputBasedPreparedStatement.class, false, new String[] { "5" }, true },
        };
    }

    private void setParameters(PreparedStatement ps, String[] params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                ps.setString(i + 1, params[i]);
            }
        }
    }

    private void checkTable(Statement stmt, String query, String[] results) throws SQLException {
        if (results == null) {
            return;
        }
        try (ResultSet rs = stmt.executeQuery(query)) {
            for (int i = 0; i < results.length; i++) {
                Assert.assertTrue(rs.next(), "Should have next row");
                Assert.assertEquals(rs.getString(1), results[i]);
            }
            Assert.assertFalse(rs.next(), "Should not have next row");
        }
    }

    @Test(groups = "integration")
    public void testQueryWithoutParameter() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement stmt = conn.prepareStatement("select 1")) {
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next(), "Should have one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");

            Assert.assertThrows(SQLException.class, () -> stmt.setInt(1, 2));
        }

        props.setProperty(JdbcConfig.PROP_NAMED_PARAM, "true");
        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement stmt = conn.prepareStatement("select 1")) {
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next(), "Should have one row");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one row");

            Assert.assertThrows(SQLException.class, () -> stmt.setInt(1, 2));
        }
    }

    @Test(groups = "integration")
    public void testReadWriteBool() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        "insert into test_read_write_bool select c1, c2 from input('c1 Int32, c2 Bool')")) {
            s.execute("drop table if exists test_read_write_bool; "
                    + "create table test_read_write_bool(id Int32, b Bool)engine=Memory");

            stmt.setInt(1, 1);
            stmt.setBoolean(2, true);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.setBoolean(2, false);
            stmt.addBatch();
            stmt.setInt(1, 3);
            stmt.setString(2, "tRUe");
            stmt.addBatch();
            stmt.setInt(1, 4);
            stmt.setString(2, "no");
            stmt.addBatch();
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 1, 1, 1, 1 });

            ResultSet rs = conn.createStatement().executeQuery("select * from test_read_write_bool order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getBoolean(2), true);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getBoolean(2), false);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 3);
            Assert.assertEquals(rs.getBoolean(2), true);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 4);
            Assert.assertEquals(rs.getBoolean(2), false);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testReadWriteBinaryString() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_binary_string; "
                    + "create table test_binary_string(id Int32, "
                    + "f0 FixedString(3), f1 Nullable(FixedString(3)), s0 String, s1 Nullable(String))engine=Memory");
        }

        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; i++) {
            bytes[i] = (byte) i;
        }
        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement ps = conn.prepareStatement("select ?, ?")) {
            ps.setBytes(1, bytes);
            ps.setString(2, Integer.toString(bytes.length));
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getBytes(1), bytes);
            Assert.assertEquals(rs.getInt(2), bytes.length);
            Assert.assertFalse(rs.next());
        }

        bytes = new byte[] { 0x61, 0x62, 0x63 };
        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement ps = conn.prepareStatement("insert into test_binary_string")) {
            ps.setInt(1, 1);
            ps.setBytes(2, bytes);
            ps.setBytes(3, null);
            ps.setBytes(4, bytes);
            ps.setBytes(5, null);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setString(2, "abc");
            ps.setString(3, null);
            ps.setString(4, "abc");
            ps.setString(5, null);
            ps.addBatch();
            ps.setInt(1, 3);
            ps.setBytes(2, bytes);
            ps.setBytes(3, bytes);
            ps.setBytes(4, bytes);
            ps.setBytes(5, bytes);
            ps.addBatch();
            ps.setInt(1, 4);
            ps.setString(2, "abc");
            ps.setString(3, "abc");
            ps.setString(4, "abc");
            ps.setString(5, "abc");
            ps.addBatch();
            ps.executeBatch();
        }

        try (ClickHouseConnection conn = newConnection(props);
                PreparedStatement ps = conn
                        .prepareStatement(
                                "select distinct * except(id) from test_binary_string where f0 = ? order by id")) {
            ps.setBytes(1, bytes);
            ResultSet rs = ps.executeQuery();
            Assert.assertTrue(rs.next(), "Should have at least one row");
            Assert.assertEquals(rs.getBytes(1), bytes);
            Assert.assertNull(rs.getBytes(2), "f1 should be null");
            Assert.assertEquals(rs.getBytes(3), bytes);
            Assert.assertNull(rs.getBytes(4), "s1 should be null");
            Assert.assertTrue(rs.next(), "Should have at least two rows");
            for (int i = 1; i <= 4; i++) {
                Assert.assertEquals(rs.getBytes(i), bytes);
                Assert.assertEquals(rs.getString(i), "abc");
            }
            Assert.assertFalse(rs.next(), "Should not have more than two rows");
        }
    }

    @Test(groups = "integration")
    public void testReadWriteDate() throws SQLException {
        LocalDate d = LocalDate.of(2021, 3, 25);
        Date x = Date.valueOf(d);
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement("insert into test_read_write_date values(? + 1,?,?)")) {
            s.execute("drop table if exists test_read_write_date");
            try {
                s.execute("create table test_read_write_date(id Int32, d1 Date, d2 Date32)engine=Memory");
            } catch (SQLException e) {
                s.execute("create table test_read_write_date(id Int32, d1 Date, d2 Nullable(Date))engine=Memory");
            }
            stmt.setInt(1, 0);
            stmt.setObject(2, d);
            stmt.setObject(3, d);
            stmt.addBatch();
            stmt.setInt(1, 1);
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
                Statement s = conn.createStatement()) {
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
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_date_cz values (?, ?, ?)")) {
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
            }

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
                PreparedStatement stmt = conn
                        .prepareStatement("insert into test_read_write_datetime values(?+1,?,?)")) {
            conn.createStatement().execute("drop table if exists test_read_write_datetime;"
                    + "create table test_read_write_datetime(id Int32, d1 DateTime32, d2 DateTime64(3))engine=Memory");
            stmt.setInt(1, 0);
            stmt.setObject(2, dt);
            stmt.setObject(3, dt);
            stmt.addBatch();
            stmt.setInt(1, 1);
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
    public void testReadWriteDateTimeWithNanos() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists test_read_write_datetime_nanos;"
                    + "CREATE TABLE test_read_write_datetime_nanos (id UUID, date DateTime64(3)) ENGINE = MergeTree() ORDER BY (id, date)");
            UUID id = UUID.randomUUID();
            long value = 1617359745321000L;
            Instant i = Instant.ofEpochMilli(value / 1000L);
            LocalDateTime dt = LocalDateTime.ofInstant(i, conn.getServerTimeZone().toZoneId());
            try (PreparedStatement ps = conn
                    .prepareStatement("insert into test_read_write_datetime_nanos values(?,?)")) {
                ps.setObject(1, id);
                ps.setObject(2, dt);
                // below works too but too slow
                // ps.setTimestamp(2, new Timestamp(value / 1000L));
                ps.executeUpdate();
            }

            ResultSet rs = stmt.executeQuery("select * from test_read_write_datetime_nanos");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), id);
            Assert.assertEquals(rs.getObject(2), dt);
            // rs.getString(2) will return "2021-04-02 03:35:45.321"
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
    public void testReadWriteEnums() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_read_write_enums;"
                    + "create table test_read_write_enums(id Int32, e1 Enum8('v1'=1,'v2'=2), e2 Enum16('v11'=11,'v22'=22))engine=Memory");
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_enums")) {
                stmt.setInt(1, 1);
                stmt.setString(2, "v1");
                stmt.setObject(3, "v11");
                stmt.addBatch();
                stmt.setInt(1, 2);
                stmt.setObject(2, 2);
                stmt.setByte(3, (byte) 22);
                stmt.addBatch();
                int[] results = stmt.executeBatch();
                Assert.assertEquals(results, new int[] { 1, 1 });
            }

            ResultSet rs = s.executeQuery("select * from test_read_write_enums order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), "v1");
            Assert.assertEquals(rs.getString(3), "v11");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getByte(2), 2);
            Assert.assertEquals(rs.getObject(3), "v22");
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testReadWriteString() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_read_write_strings;"
                    + "create table test_read_write_strings(id Int32, s1 String, s2 Nullable(String), s3 Array(String), s4 Array(Nullable(String)))engine=Memory");
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_strings")) {
                stmt.setInt(1, 0);
                stmt.setObject(2, null);
                stmt.setObject(3, null);
                stmt.setObject(4, new String[0]);
                stmt.setObject(5, new String[0]);
                Assert.assertThrows(RuntimeException.class, () -> stmt.execute());
            }
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_strings")) {
                stmt.setInt(1, 1);
                stmt.setObject(2, "");
                stmt.setString(3, "");
                stmt.setArray(4, conn.createArrayOf("String", new String[] { "" }));
                stmt.setObject(5, new String[] { "" });
                stmt.addBatch();
                stmt.setInt(1, 2);
                stmt.setString(2, "");
                stmt.setString(3, null);
                stmt.setObject(4, new String[0]);
                stmt.setArray(5, conn.createArrayOf("String", new String[] { null }));
                stmt.addBatch();
                int[] results = stmt.executeBatch();
                Assert.assertEquals(results, new int[] { 1, 1 });
            }

            ResultSet rs = s.executeQuery("select * from test_read_write_strings order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getString(2), "");
            Assert.assertEquals(rs.getObject(3), "");
            Assert.assertEquals(rs.getObject(4), new String[] { "" });
            Assert.assertEquals(rs.getArray(5).getArray(), new String[] { "" });
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), "");
            Assert.assertEquals(rs.getString(3), null);
            Assert.assertEquals(rs.getArray(4).getArray(), new String[0]);
            Assert.assertEquals(rs.getObject(5), new String[] { null });
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
            Instant i = Instant.ofEpochMilli(value / 1000L);
            LocalDateTime dt = LocalDateTime.ofInstant(i, conn.getServerTimeZone().toZoneId());
            try (PreparedStatement ps = conn.prepareStatement("insert into test_issue_612 values(trim(?),?)")) {
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
                Assert.assertEquals(rs.getObject(2), dt);
                Assert.assertEquals(rs.getLong(2), dt.atZone(conn.getServerTimeZone().toZoneId()).toEpochSecond());
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
                PreparedStatement stmt = conn.prepareStatement("insert into test_batch_insert values(? + 1,?)")) {
            s.execute("drop table if exists test_batch_insert;"
                    + "create table test_batch_insert(id Int32, name Nullable(String))engine=Memory");
            stmt.setInt(1, 0);
            stmt.setString(2, "a");
            stmt.addBatch();
            stmt.setInt(1, 1);
            stmt.setString(2, "b");
            stmt.addBatch();
            stmt.setInt(1, 2);
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
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
        }
    }

    @Test(dataProvider = "statementAndParams", groups = "integration")
    public void testExecuteWithOrWithoutParameters(String tableSuffix, String query, Class<?> clazz,
            boolean hasResultSet, String[] params, boolean checkTable) throws SQLException {
        String tableName = "test_execute_ps_" + tableSuffix;
        query = query.replace("$table", tableName);
        Properties props = new Properties();
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute("drop table if exists " + tableName
                    + "; create table " + tableName + "(s String)engine=Memory"), "Should not have result set");

            try (PreparedStatement ps = conn.prepareStatement(query)) {
                Assert.assertEquals(ps.getClass(), clazz);

                // executeQuery
                setParameters(ps, params);
                Assert.assertNotNull(ps.executeQuery(), "executeQuery should never return null result set");
                if (hasResultSet) {
                    Assert.assertNotNull(ps.getResultSet(), "Should have result set");
                    Assert.assertEquals(ps.getUpdateCount(), -1);
                    Assert.assertEquals(ps.getLargeUpdateCount(), -1L);
                } else {
                    Assert.assertNull(ps.getResultSet(), "Should not have result set");
                    Assert.assertTrue(ps.getUpdateCount() >= 0, "Should have update count");
                    Assert.assertTrue(ps.getLargeUpdateCount() >= 0L, "Should have update count");
                }
                if (checkTable)
                    checkTable(stmt, "select * from " + tableName, params);

                // execute
                Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
                setParameters(ps, params);
                if (hasResultSet) {
                    Assert.assertTrue(ps.execute(), "Should have result set");
                    Assert.assertNotNull(ps.getResultSet(), "Should have result set");
                    Assert.assertEquals(ps.getUpdateCount(), -1);
                    Assert.assertEquals(ps.getLargeUpdateCount(), -1L);
                } else {
                    Assert.assertFalse(ps.execute(), "Should not have result set");
                    Assert.assertNull(ps.getResultSet(), "Should not have result set");
                    Assert.assertTrue(ps.getUpdateCount() >= 0, "Should have update count");
                    Assert.assertTrue(ps.getLargeUpdateCount() >= 0L, "Should have update count");
                }
                if (checkTable)
                    checkTable(stmt, "select * from " + tableName, params);

                // executeLargeUpdate
                Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
                setParameters(ps, params);
                Assert.assertEquals(ps.executeLargeUpdate(), ps.getLargeUpdateCount());
                if (hasResultSet) {
                    Assert.assertNotNull(ps.getResultSet(), "Should have result set");
                    Assert.assertEquals(ps.getUpdateCount(), -1);
                    Assert.assertEquals(ps.getLargeUpdateCount(), -1L);
                } else {
                    Assert.assertNull(ps.getResultSet(), "Should not have result set");
                    Assert.assertTrue(ps.getUpdateCount() >= 0, "Should have update count");
                    Assert.assertTrue(ps.getLargeUpdateCount() >= 0L, "Should have update count");
                }
                if (checkTable)
                    checkTable(stmt, "select * from " + tableName, params);

                // executeUpdate
                Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
                setParameters(ps, params);
                Assert.assertEquals(ps.executeUpdate(), ps.getUpdateCount());
                if (hasResultSet) {
                    Assert.assertNotNull(ps.getResultSet(), "Should have result set");
                    Assert.assertEquals(ps.getUpdateCount(), -1);
                    Assert.assertEquals(ps.getLargeUpdateCount(), -1L);
                } else {
                    Assert.assertNull(ps.getResultSet(), "Should not have result set");
                    Assert.assertTrue(ps.getUpdateCount() >= 0, "Should have update count");
                    Assert.assertTrue(ps.getLargeUpdateCount() >= 0L, "Should have update count");
                }
                if (checkTable)
                    checkTable(stmt, "select * from " + tableName, params);
            }

            // executeLargeBatch
            Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                Assert.assertEquals(ps.getClass(), clazz);
                setParameters(ps, params);
                ps.addBatch();
                Assert.assertThrows(SQLException.class, () -> ps.execute());
                Assert.assertThrows(SQLException.class, () -> ps.executeQuery());
                Assert.assertThrows(SQLException.class, () -> ps.executeUpdate());
                if (hasResultSet) {
                    Assert.assertThrows(SQLException.class, () -> ps.executeLargeBatch());
                } else {
                    Assert.assertEquals(ps.executeLargeBatch(), new long[] { 1L });
                }
                if (checkTable)
                    checkTable(stmt, "select * from " + tableName, params);
            }

            // executeBatch
            Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
            try (PreparedStatement ps = conn.prepareStatement(query)) {
                Assert.assertEquals(ps.getClass(), clazz);
                setParameters(ps, params);
                ps.addBatch();
                Assert.assertThrows(SQLException.class, () -> ps.execute());
                Assert.assertThrows(SQLException.class, () -> ps.executeQuery());
                Assert.assertThrows(SQLException.class, () -> ps.executeUpdate());
                if (hasResultSet) {
                    Assert.assertThrows(SQLException.class, () -> ps.executeBatch());
                } else {
                    Assert.assertEquals(ps.executeBatch(), new int[] { 1 });
                }
                if (checkTable)
                    checkTable(stmt, "select * from " + tableName, params);
            }
        }

        props.setProperty(JdbcConfig.PROP_CONTINUE_BATCH, "true");
        try (Connection conn = newConnection(props);
                Statement stmt = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement(query)) {
            Assert.assertEquals(ps.getClass(), clazz);

            // executeLargeBatch
            Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
            setParameters(ps, params);
            ps.addBatch();
            Assert.assertThrows(SQLException.class, () -> ps.execute());
            Assert.assertThrows(SQLException.class, () -> ps.executeQuery());
            Assert.assertThrows(SQLException.class, () -> ps.executeUpdate());
            if (hasResultSet) {
                Assert.assertEquals(ps.executeLargeBatch(), new long[] { Statement.EXECUTE_FAILED });
            } else {
                Assert.assertEquals(ps.executeLargeBatch(), new long[] { 1L });
            }
            if (checkTable)
                checkTable(stmt, "select * from " + tableName, params);

            // executeBatch
            Assert.assertFalse(stmt.execute("truncate table " + tableName), "Should not have result set");
            setParameters(ps, params);
            ps.addBatch();
            Assert.assertThrows(SQLException.class, () -> ps.execute());
            Assert.assertThrows(SQLException.class, () -> ps.executeQuery());
            Assert.assertThrows(SQLException.class, () -> ps.executeUpdate());
            if (hasResultSet) {
                Assert.assertEquals(ps.executeBatch(), new int[] { Statement.EXECUTE_FAILED });
            } else {
                Assert.assertEquals(ps.executeBatch(), new int[] { 1 });
            }
            if (checkTable)
                checkTable(stmt, "select * from " + tableName, params);
        }
    }

    @Test(groups = "integration")
    public void testLoadRawData() throws Exception {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement stmt = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement(
                        "insert into test_jdbc_load_raw_data select * from {tt 'raw_data'}")) {
            Assert.assertFalse(stmt.execute("drop table if exists test_jdbc_load_raw_data; "
                    + "create table test_jdbc_load_raw_data(s String)engine=Memory"), "Should not have result set");
            ClickHouseConfig config = stmt.getConfig();
            CompletableFuture<Integer> future;
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, null)) {
                ps.setObject(1, ClickHouseExternalTable.builder().name("raw_data")
                        .columns("s String").format(ClickHouseFormat.RowBinary)
                        .content(stream.getInputStream())
                        .build());
                future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return ps.executeUpdate();
                    } catch (SQLException e) {
                        throw new CompletionException(e);
                    }
                });

                // write bytes into the piped stream
                for (int i = 0; i < 101; i++) {
                    stream.writeAsciiString(Integer.toString(i));
                }
            }

            Assert.assertTrue(future.get() >= 0);
        }
    }

    @Test(groups = "integration")
    public void testQueryWithExternalTable() throws SQLException {
        // FIXME grpc seems has problem dealing with session
        if (DEFAULT_PROTOCOL == ClickHouseProtocol.GRPC) {
            return;
        }

        try (ClickHouseConnection conn = newConnection(new Properties());
                PreparedStatement stmt = conn
                        .prepareStatement("SELECT count() FROM (select 3 x) WHERE x IN {tt 'table1' }")) {
            stmt.setObject(1, ClickHouseExternalTable.builder().name("table1").columns("id Int8")
                    .format(ClickHouseFormat.TSV)
                    .content(new ByteArrayInputStream("1".getBytes(StandardCharsets.US_ASCII)))
                    .build());
            ResultSet rs = stmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 0);
            Assert.assertFalse(rs.next());
        }

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

    @Test(dataProvider = "nonBatchQueries", groups = "integration")
    public void testNonBatchUpdate(String mode, String query) throws SQLException {
        String tableName = String.format("test_non_batch_update_%s", mode);
        try (Connection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement();) {
            stmt.execute(String.format("drop table if exists %1$s; "
                    + "create table %1$s (id Int32, str String)engine=Memory", tableName));

            try (PreparedStatement ps = conn.prepareStatement(String.format(query, tableName))) {
                if (query.contains("{tt ")) {
                    ps.setObject(1,
                            ClickHouseExternalTable.builder().name("tbl").columns("id Int32, str String")
                                    .content(ClickHouseInputStream.of(
                                            Collections.singleton("1\t1\n".getBytes()), byte[].class, null,
                                            null))
                                    .build());
                } else {
                    ps.setInt(1, 1);
                    ps.setString(2, "1");
                }
                Assert.assertEquals(ps.executeUpdate(), 1);
            }

            // insertion was a success
            try (ResultSet rs = stmt.executeQuery(String.format("select * from %s", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "1");
                Assert.assertEquals(rs.getInt(2), 1);
                Assert.assertFalse(rs.next());
            }

            // make sure it won't throw BatchUpdateException
            try (PreparedStatement ps = conn.prepareStatement(String.format(query, tableName))) {
                if (query.contains("{tt ")) {
                    ps.setObject(1,
                            ClickHouseExternalTable.builder().name("tbl").columns("id Int32, str String")
                                    .content(ClickHouseInputStream.of(
                                            Collections.singleton("2\t2\n".getBytes()), byte[].class, null,
                                            null))
                                    .build());
                } else {
                    ps.setInt(1, 2);
                    ps.setString(2, "2");
                }
                stmt.execute(String.format("drop table %s", tableName));

                SQLException exp = null;
                try {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    exp = e;
                }
                Assert.assertTrue(exp.getClass() == SQLException.class);
            }
        }
    }

    @Test(dataProvider = "columnsWithDefaultValue", groups = "integration")
    public void testInsertDefaultValue(String columnType, String defaultExpr, String defaultValue) throws Exception {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.COMPRESS.getKey(), "false");
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(),
                ClickHouseFormat.TabSeparatedWithNamesAndTypes.name());
        String tableName = "test_insert_default_value_" + columnType.split("\\(")[0].trim().toLowerCase();
        try (ClickHouseConnection conn = newConnection(props); Statement s = conn.createStatement()) {
            if (conn.getUri().toString().contains(":grpc:")) {
                throw new SkipException("Skip gRPC test");
            } else if (!conn.getServerVersion().check("[21.8,)")) {
                throw new SkipException("Skip test when ClickHouse is older than 21.8");
            }
            s.execute(String.format("drop table if exists %s; ", tableName)
                    + String.format("create table %s(id Int8, v %s DEFAULT %s)engine=Memory", tableName, columnType,
                            defaultExpr));
            s.executeUpdate(String.format("insert into %s values(1, null)", tableName));
            try (PreparedStatement stmt = conn
                    .prepareStatement(String.format("insert into %s values(?,?)", tableName))) {
                stmt.setInt(1, 2);
                stmt.setObject(2, null);
                stmt.executeUpdate();
                stmt.setInt(1, 3);
                stmt.setNull(2, Types.OTHER);
                stmt.executeUpdate();
            }

            int rowCount = 0;
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id", tableName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), defaultValue);
                Assert.assertFalse(rs.wasNull(), "Should not be null");
                rowCount++;
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(rs.getString(2), defaultValue);
                Assert.assertFalse(rs.wasNull(), "Should not be null");
                rowCount++;
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getString(2), defaultValue);
                Assert.assertFalse(rs.wasNull(), "Should not be null");
                rowCount++;
                Assert.assertFalse(rs.next(), "Should have only 3 rows");
            }
            Assert.assertEquals(rowCount, 3);
        } catch (SQLException e) {
            // 'Unknown data type family', 'Missing columns' or 'Cannot create table column'
            if (e.getErrorCode() == 50 || e.getErrorCode() == 47 || e.getErrorCode() == 44) {
                return;
            }
            throw e;
        }
    }

    @Test(groups = "integration")
    public void testInsertStringAsArray() throws Exception {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        "insert into test_array_insert(id, a, b) values (toUInt32(?),?,?)")) {
            s.execute("drop table if exists test_array_insert;"
                    + "create table test_array_insert(id UInt32, a Array(Int16), b Array(Nullable(UInt32)))engine=Memory");

            stmt.setString(1, "1");
            stmt.setString(2, "[1,2,3]");
            stmt.setString(3, "[3,null,1]");
            Assert.assertEquals(stmt.executeUpdate(), 1);

            ResultSet rs = s.executeQuery("select * from test_array_insert order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), new short[] { 1, 2, 3 });
            Assert.assertEquals(rs.getObject(3), new Long[] { 3L, null, 1L });
            Assert.assertFalse(rs.next());
        }

        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_string_array_insert; "
                    + "create table test_string_array_insert(id UInt32, a Array(LowCardinality(String)), b Array(Nullable(String)))engine=Memory");

            try (PreparedStatement stmt = conn.prepareStatement(
                    "insert into test_string_array_insert(id, a, b) values (?,?,?)")) {
                stmt.setString(1, "1");
                stmt.setObject(2, new String[] { "1", "2", "3" });
                stmt.setArray(3, conn.createArrayOf("String", new String[] { "3", null, "1" }));
                Assert.assertEquals(stmt.executeUpdate(), 1);
            }

            ResultSet rs = s.executeQuery("select * from test_string_array_insert order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getArray(2).getArray(), new String[] { "1", "2", "3" });
            Assert.assertEquals(rs.getObject(3), new String[] { "3", null, "1" });
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testInsertWithFunction() throws Exception {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement stmt = conn.prepareStatement(
                        conn.getServerVersion().check("[22.3,)")
                                ? "insert into test_issue_315(id, src, dst) values (?+0,?,?)"
                                : "insert into test_issue_315(id, src, dst) values (?,IPv4ToIPv6(toIPv4(?)),IPv4ToIPv6(toIPv4(?)))")) {
            s.execute("drop table if exists test_issue_315; "
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
            Assert.assertEquals(rs.getString(3),
                    conn.getServerVersion().check("[22.3,)") ? "0:0:0:0:0:0:0:1" : "0:0:0:0:0:ffff:0:0");
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

    @Test(groups = "integration")
    public void testInsertWithAndSelect() throws Exception {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_insert_with_and_select; "
                    + "CREATE TABLE test_insert_with_and_select(value String) ENGINE=Memory");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO test_insert_with_and_select(value) WITH t as ( SELECT 'testValue1') SELECT * FROM t")) {
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO test_insert_with_and_select(value) WITH t as ( SELECT 'testValue2' as value) SELECT * FROM t WHERE value != ?")) {
                ps.setString(1, "");
                ps.executeUpdate();
            }

            ResultSet rs = s.executeQuery("select * from test_insert_with_and_select order by value");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString("Value"), "testValue1");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString("VALUE"), "testValue2");
            Assert.assertFalse(rs.next());
        }
    }
}
