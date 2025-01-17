package com.clickhouse.jdbc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseIntegerValue;
import com.clickhouse.data.value.ClickHouseNestedValue;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.array.ClickHouseByteArrayValue;
import com.clickhouse.jdbc.internal.InputBasedPreparedStatement;
import com.clickhouse.jdbc.internal.SqlBasedPreparedStatement;
import com.clickhouse.jdbc.internal.StreamBasedPreparedStatement;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

public class ClickHousePreparedStatementTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
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

    @DataProvider(name = "columnsWithoutDefaultValue")
    private Object[][] getColumnsWithoutDefaultValue() {
        return new Object[][] {
                new Object[] { "Bool", "false" },
                new Object[] { "Date", "1970-01-01" },
                new Object[] { "Date32", "1970-01-01" },
                new Object[] { "DateTime32('UTC')", "1970-01-01 00:00:00" },
                new Object[] { "DateTime64(3, 'UTC')", "1970-01-01 00:00:00" },
                new Object[] { "Decimal(10,4)", "0" },
                new Object[] { "Enum8('x'=0,'y'=1)", "x" },
                new Object[] { "Enum16('xx'=1,'yy'=0)", "yy" },
                new Object[] { "Float32", "0.0" },
                new Object[] { "Float64", "0.0" },
                new Object[] { "Int8", "0" },
                new Object[] { "UInt8", "0" },
                new Object[] { "Int16", "0" },
                new Object[] { "UInt16", "0" },
                new Object[] { "Int32", "0" },
                new Object[] { "UInt32", "0" },
                new Object[] { "Int64", "0" },
                new Object[] { "UInt64", "0" },
                new Object[] { "Int128", "0" },
                new Object[] { "UInt128", "0" },
                new Object[] { "Int256", "0" },
                new Object[] { "UInt256", "0" },
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
        props.setProperty(ClickHouseClientOption.USE_BINARY_STRING.getKey(), "true");
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_binary_string; "
                    + "create table test_binary_string(id Int32, "
                    + "f0 FixedString(3), f1 Nullable(FixedString(3)), s0 String, s1 Nullable(String)) engine=MergeTree ORDER BY id");
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
                        .prepareStatement("SELECT DISTINCT * EXCEPT(id) FROM test_binary_string" +
                                " WHERE f0 = ? ORDER BY id" + (isCloud() ? " SETTINGS select_sequential_consistency=1" : ""))) {
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
    public void testReadWriteArrayWithNullableTypes() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_read_write_nullable_unsigned_types;"
                    + "create table test_read_write_nullable_unsigned_types(id Int32, a1 Array(Nullable(Int8)), a2 Array(Nullable(UInt64)))engine=Memory");
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_nullable_unsigned_types")) {
                stmt.setInt(1, 1);
                stmt.setObject(2, new byte[0]);
                stmt.setObject(3, new long[0]);
                stmt.execute();
            }
            try (PreparedStatement stmt = conn
                    .prepareStatement("insert into test_read_write_nullable_unsigned_types")) {
                stmt.setInt(1, 2);
                stmt.setObject(2, new byte[] { 2, 2 });
                stmt.setObject(3, new Long[] { 2L, null });
                stmt.addBatch();
                stmt.setInt(1, 3);
                stmt.setArray(2, conn.createArrayOf("Nullable(Int8)", new Byte[] { null, 3 }));
                stmt.setArray(3,
                        conn.createArrayOf("Nullable(UInt64)", new UnsignedLong[] { null, UnsignedLong.valueOf(3L) }));
                stmt.addBatch();
                int[] results = stmt.executeBatch();
                Assert.assertEquals(results, new int[] { 1, 1 });
            }

            ResultSet rs = s.executeQuery("select * from test_read_write_nullable_unsigned_types order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), new Byte[0]);
            Assert.assertEquals(rs.getArray(2).getArray(), new Byte[0]);
            Assert.assertEquals(rs.getObject(3), new UnsignedLong[0]);
            Assert.assertEquals(rs.getArray(3).getArray(), new UnsignedLong[0]);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), new Byte[] { 2, 2 });
            Assert.assertEquals(rs.getArray(2).getArray(), new Byte[] { 2, 2 });
            Assert.assertEquals(rs.getObject(3), new UnsignedLong[] { UnsignedLong.valueOf(2L), null });
            Assert.assertEquals(rs.getArray(3).getArray(), new UnsignedLong[] { UnsignedLong.valueOf(2L), null });
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 3);
            Assert.assertEquals(rs.getObject(2), new Byte[] { null, 3 });
            Assert.assertEquals(rs.getArray(2).getArray(), new Byte[] { null, 3 });
            Assert.assertEquals(rs.getObject(3), new UnsignedLong[] { null, UnsignedLong.valueOf(3L) });
            Assert.assertEquals(rs.getArray(3).getArray(), new UnsignedLong[] { null, UnsignedLong.valueOf(3L) });
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
                Assert.assertThrows(SQLException.class, () -> stmt.execute());
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
    public void testBatchDdl() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props)) {
            if (!conn.getServerVersion().check("[22.8,)")) {
                throw new SkipException("Skip due to error 'unknown key zookeeper_load_balancing'");
            }
            try (PreparedStatement stmt = conn.prepareStatement(isCloud() ?
                    "drop table if exists test_batch_dll" :
                    "drop table if exists test_batch_dll_on_cluster on cluster single_node_cluster_localhost")) {
                stmt.addBatch();
                stmt.addBatch();
                Assert.assertEquals(stmt.executeBatch(), new int[] { 0, 0 });
            }

            try (PreparedStatement stmt = conn.prepareStatement("select 1")) {
                stmt.addBatch();
                Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
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
    public void testBatchInsertWithoutUnboundedQueue() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.WRITE_BUFFER_SIZE.getKey(), "1");
        props.setProperty(ClickHouseClientOption.MAX_QUEUED_BUFFERS.getKey(), "1");
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_insert_buffer_size; "
                    + "CREATE TABLE test_insert_buffer_size(value String) ENGINE=Memory");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO test_insert_buffer_size")) {
                ps.setString(1, "1");
                ps.addBatch();
                ps.setString(1, "2");
                ps.addBatch();
                ps.setString(1, "3");
                ps.addBatch();
                ps.executeBatch();

                ps.setString(1, "4");
                ps.addBatch();
                ps.executeBatch();

                ps.setString(1, "4");
                ps.addBatch();
                ps.clearBatch();
                ps.setString(1, "5");
                ps.addBatch();
                ps.setString(1, "6");
                ps.addBatch();
                ps.executeBatch();
            }

            try (ResultSet rs = s.executeQuery("select * from test_insert_buffer_size order by value")) {
                int count = 1;
                while (rs.next()) {
                    Assert.assertEquals(rs.getInt(1), count++);
                }
                Assert.assertEquals(count, 7);
            }
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
            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(0, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.setInt(2, 5));
            Assert.assertThrows(SQLException.class, () -> stmt.addBatch());

            stmt.setInt(1, 3);
            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);
            stmt.addBatch();
            stmt.setInt(1, 2);
            stmt.addBatch();
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);
        }
    }

    @Test(dataProvider = "statementAndParams", groups = "integration")
    public void testExecuteWithOrWithoutParameters(String tableSuffix, String query, Class<?> clazz,
            boolean hasResultSet, String[] params, boolean checkTable) throws SQLException {
        int expectedRowCount = "ddl".equals(tableSuffix) ? 0 : 1;
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
                    Assert.assertEquals(ps.executeLargeBatch(), new long[] { expectedRowCount });
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
                    Assert.assertEquals(ps.executeBatch(), new int[] { expectedRowCount });
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
                Assert.assertEquals(ps.executeLargeBatch(), new long[] { expectedRowCount });
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
                Assert.assertEquals(ps.executeBatch(), new int[] { expectedRowCount });
            }
            if (checkTable)
                checkTable(stmt, "select * from " + tableName, params);
        }
    }

    @Test(groups = "integration")
    public void testLoadRawData() throws IOException, SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement stmt = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement(
                        "insert into test_jdbc_load_raw_data select * from {tt 'raw_data'}")) {
            Assert.assertFalse(stmt.execute("drop table if exists test_jdbc_load_raw_data; "
                    + "create table test_jdbc_load_raw_data(s String)engine=Memory"), "Should not have result set");
            ClickHouseConfig config = stmt.getConfig();
            CompletableFuture<Integer> future;
            try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config)) {
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

            try {
                Assert.assertTrue(future.get() >= 0);
            } catch (InterruptedException | ExecutionException ex) {
                Assert.fail("Failed to get result", ex);
            }
        }
    }

    @Test(groups = "integration")
    public void testQueryWithExternalTable() throws SQLException {
        if (isCloud()) return; //TODO: testQueryWithExternalTable - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
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

    @Test(groups = "integration")
    public void testInsertAggregateFunction() throws SQLException {
        // https://kb.altinity.com/altinity-kb-schema-design/ingestion-aggregate-function/
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement(
                        "insert into test_insert_aggregate_function SELECT uid, updated, arrayReduce('argMaxState', [name], [updated]) "
                                + "FROM input('uid Int16, updated DateTime, name String')")) {
            s.execute("drop table if exists test_insert_aggregate_function;"
                    + "CREATE TABLE test_insert_aggregate_function (uid Int16, updated SimpleAggregateFunction(max, DateTime), "
                    + "name AggregateFunction(argMax, String, DateTime)) ENGINE=AggregatingMergeTree order by uid");
            ps.setInt(1, 1);
            ps.setString(2, "2020-01-02 00:00:00");
            ps.setString(3, "b");
            ps.addBatch();
            ps.setInt(1, 1);
            ps.setString(2, "2020-01-01 00:00:00");
            ps.setString(3, "a");
            ps.addBatch();
            ps.executeBatch();
            try (ResultSet rs = s.executeQuery(
                    "select uid, max(updated) AS updated, argMaxMerge(name) from test_insert_aggregate_function group by uid")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "2020-01-02 00:00:00");
                Assert.assertEquals(rs.getString(3), "b");
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration")
    public void testInsertByteArray() throws SQLException {
        Properties props = new Properties();
        props.setProperty("use_binary_string", "true");
        try (ClickHouseConnection conn = newConnection(props); Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_insert_byte_array;"
                    + "create table test_insert_byte_array(id String, b Array(Int8), s Array(Array(Int8))) engine=Memory");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "insert into test_insert_byte_array(id, b, s) values (?,?,?)")) {
                stmt.setString(1, "1");
                stmt.setObject(2, new byte[] { 1, 2, 3 });
                stmt.setObject(3, new byte[][] { { 1, 2, 3 }, { 4, 5, 6 } });
                Assert.assertEquals(stmt.executeUpdate(), 1);

                ResultSet rs = s.executeQuery("select * from test_insert_byte_array order by id");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getObject(2), new byte[] { 1, 2, 3 });
                Assert.assertEquals(rs.getObject(3), new byte[][] { { 1, 2, 3 }, { 4, 5, 6 } });
                Assert.assertFalse(rs.next());
            }
        }
    }

    //TODO: Revisit
    @Test(groups = "integration")
    public void testInsertDefaultValue() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO test_insert_default_value select id, name from input('id UInt32, name Nullable(String)')")) {
            s.execute("DROP TABLE IF EXISTS test_insert_default_value; CREATE TABLE test_insert_default_value(n Int32, s String DEFAULT 'secret') engine=MergeTree ORDER BY n");
            ps.setInt(1, 1);
            ps.setString(2, null);
            ps.addBatch();
            ps.setInt(1, -1);
            ps.setNull(2, Types.ARRAY);
            ps.addBatch();
            ps.executeBatch();
            try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM test_insert_default_value ORDER BY n %s", isCloud() ? "SETTINGS select_sequential_consistency=1" : ""))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), -1);
                Assert.assertEquals(rs.getString(2), "secret");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "secret");
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration", enabled = false)
    public void testOutFileAndInFile() throws SQLException {
        if (isCloud()) return; //TODO: testOutFileAndInFile - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        if (DEFAULT_PROTOCOL != ClickHouseProtocol.HTTP) {
            throw new SkipException("Skip non-http protocol");
        }

        Properties props = new Properties();
        props.setProperty("localFile", "true");
        File f = new File("f.csv");
        if (f.exists()) {
            f.delete();
        }
        f.deleteOnExit();
        try (ClickHouseConnection conn = newConnection(props); Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_load_infile_with_params;"
                    + "CREATE TABLE test_load_infile_with_params(n Int32, s String) engine=Memory");
            try (PreparedStatement stmt = conn
                    .prepareStatement("SELECT number n, toString(n) from numbers(999) into outfile ?")) {
                stmt.setString(1, f.getName());
                try (ResultSet rs = stmt.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString(1), f.getName());
                    Assert.assertFalse(rs.next());
                }
                Assert.assertTrue(f.exists());

                stmt.setString(1, f.getName());
                Assert.assertThrows(SQLException.class, () -> stmt.executeQuery());
                stmt.setString(1, f.getName() + "!");
                try (ResultSet rs = stmt.executeQuery()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString(1), f.getName());
                    Assert.assertFalse(rs.next());
                }
            }

            try (PreparedStatement stmt = conn
                    .prepareStatement("INSERT INTO test_load_infile_with_params FROM infile ? format CSV")) {
                stmt.setString(1, f.getName());
                stmt.addBatch();
                stmt.setString(1, f.getName());
                stmt.addBatch();
                stmt.setString(1, f.getName() + "!");
                stmt.addBatch();
                stmt.executeBatch();
            }

            try (ResultSet rs = s.executeQuery("SELECT count(1), uniqExact(n) FROM test_load_infile_with_params")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 999 * 3);
                Assert.assertEquals(rs.getInt(2), 999);
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        }
    }

    @Test(dataProvider = "columnsWithDefaultValue", groups = "integration")
    public void testInsertDefaultValue(String columnType, String defaultExpr, String defaultValue) throws SQLException {
        Properties props = new Properties();
        props.setProperty(JdbcConfig.PROP_NULL_AS_DEFAULT, "1");
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
                    + String.format("CREATE TABLE %s(id Int8, v %s DEFAULT %s) engine=MergeTree ORDER BY id", tableName, columnType,
                            defaultExpr));
            s.executeUpdate(String.format("INSERT INTO %s values(1, null)", tableName));
            try (PreparedStatement stmt = conn
                    .prepareStatement(String.format("insert into %s values(?,?)", tableName))) {
                stmt.setInt(1, 2);
                stmt.setObject(2, null);
                stmt.executeUpdate();
                stmt.setInt(1, 3);
                stmt.setNull(2, Types.OTHER);
                stmt.executeUpdate();
            } catch (Exception e) {
                if (e.getMessage().contains("Unexpected type on mixNumberColumns")) {
                    return;
                }
            }

            int rowCount = 0;
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id %s", tableName, isCloud() ? "SETTINGS select_sequential_consistency=1" : ""))) {
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
    public void testInsertNestedValue() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties()); Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_nested_insert;"
                    + "create table test_nested_insert(id UInt32, n Nested(c1 Int8, c2 Int8))engine=Memory");
            try (PreparedStatement ps = conn.prepareStatement("insert into test_nested_insert")) {
                // insert into test_nested_insert values(0, [],[])
                ps.setObject(1, 0);
                ps.setObject(2, new int[0]);
                ps.setObject(3, new int[0]);
                Assert.assertEquals(ps.executeUpdate(), 1);

                // insert into test_nested_insert values(1, [1],[1])
                ps.setInt(1, 1);
                ps.setBytes(2, new byte[] { 1 });
                ps.setArray(3, conn.createArrayOf("Array(Int8)", new Byte[] { 1 }));
                Assert.assertEquals(ps.executeUpdate(), 1);

                // insert into test_nested_insert values(2, [1,2],[1,2])
                ps.setObject(1, ClickHouseIntegerValue.of(2));
                ps.setObject(2, ClickHouseByteArrayValue.of(new byte[] { 1, 2 }));
                ps.setObject(3, ClickHouseByteArrayValue.of(new byte[] { 1, 2 }));
                Assert.assertEquals(ps.executeUpdate(), 1);

                // insert into test_nested_insert values(3, [1,2,3],[1,2,3])
                ps.setString(1, "3");
                ps.setString(2, "[1,2,3]");
                ps.setString(3, "[1,2,3]");
                Assert.assertEquals(ps.executeUpdate(), 1);
            }

            try (ResultSet rs = s.executeQuery("select * from test_nested_insert order by id")) {
                for (int i = 0; i < 4; i++) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), i);
                    byte[] bytes = new byte[i];
                    for (int j = 0; j < i; j++) {
                        bytes[j] = (byte) (j + 1);
                    }
                    Assert.assertEquals(rs.getObject(2), bytes);
                    Assert.assertEquals(rs.getObject(3), bytes);
                }
                Assert.assertFalse(rs.next());
            }

            // same case but this time disable flatten_nested
            s.execute("set flatten_nested=0; drop table if exists test_nested_insert; "
                    + "create table test_nested_insert(id UInt32, n Nested(c1 Int8, c2 Int8))engine=Memory");
            try (PreparedStatement ps = conn.prepareStatement("insert into test_nested_insert")) {
                // insert into test_nested_insert values(0, [])
                ps.setObject(1, 0);
                ps.setObject(2, new Integer[0][]);
                Assert.assertEquals(ps.executeUpdate(), 1);

                // insert into test_nested_insert values(1, [(1,1)])
                ps.setInt(1, 1);
                ps.setArray(2, conn.createArrayOf("Array(Array(Int8))", new Byte[][] { { 1, 1 } }));
                Assert.assertEquals(ps.executeUpdate(), 1);

                // insert into test_nested_insert values(2, [(1,1),(2,2)])
                ps.setObject(1, ClickHouseIntegerValue.of(2));
                ps.setObject(2,
                        ClickHouseNestedValue.of(
                                ClickHouseColumn.of("n", "Nested(c1 Int8, c2 Int8)").getNestedColumns(),
                                new Byte[][] { { 1, 1 }, { 2, 2 } }));
                Assert.assertEquals(ps.executeUpdate(), 1);

                // insert into test_nested_insert values(3, [(1,1),(2,2),(3,3)])
                ps.setString(1, "3");
                // ps.setString(2, "[(1,1),(2,2),(3,3)]");
                ps.setObject(2,
                        ClickHouseNestedValue.of(
                                ClickHouseColumn.of("n", "Nested(c1 Int8, c2 Int8)").getNestedColumns(),
                                new Byte[][] { { 1, 1 }, { 2, 2 }, { 3, 3 } }));
                Assert.assertEquals(ps.executeUpdate(), 1);
            }

            try (ResultSet rs = s.executeQuery("select * from test_nested_insert order by id")) {
                for (int i = 0; i < 4; i++) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), i);
                    Byte[][] bytes = new Byte[i][];
                    for (int j = 0; j < i; j++) {
                        Byte[] b = new Byte[2];
                        for (int k = 0; k < 2; k++) {
                            b[k] = (byte) (j + 1);
                        }
                        bytes[j] = b;
                    }
                    Assert.assertEquals(rs.getObject(2), bytes);
                }
                Assert.assertFalse(rs.next());
            }

            // https://github.com/ClickHouse/clickhouse-java/issues/1259
            s.execute("set flatten_nested=0; drop table if exists test_nested_insert; "
                    + "create table test_nested_insert(id UInt32, n Nested(c1 Int8, c2 LowCardinality(String)))engine=Memory");
            try (PreparedStatement ps = conn.prepareStatement("insert into test_nested_insert(id, n)")) {
                ps.setString(1, "1");
                ps.setObject(2, new Object[][] { { 1, "foo1" }, { 2, "bar1" }, { 3, "bug1" } });
                ps.executeUpdate();
            }
            // try invalid query
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into test_nested_insert(id, n) select id, n from input('id UInt32, n Nested(c1 Int8, c2 LowCardinality(String)))'")) {
                ps.setString(1, "2");
                ps.setObject(2, new Object[][] { { 4, "foo2" }, { 5, "bar2" }, { 6, "bug2" } });
                ps.executeUpdate();
                Assert.fail("Query should fail");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().startsWith("Missing "));
            }
            // now use input function
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into test_nested_insert(id, n) select id, n from input('id UInt32, n Nested(c1 Int8, c2 LowCardinality(String))') settings flatten_nested=0")) {
                ps.setString(1, "2");
                ps.setObject(2, new Object[][] { { 4, "foo2" }, { 5, "bar2" }, { 6, "bug2" } });
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("select * from test_nested_insert order by id")) {
                for (int i = 1; i <= 2; i++) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), i);
                    Object[][] nestedValue = (Object[][]) rs.getObject(2);
                    Assert.assertEquals(nestedValue.length, 3);
                    String[] arr = new String[] { "foo", "bar", "bug" };
                    for (int j = 1; j <= 3; j++) {
                        Assert.assertEquals(nestedValue[j - 1],
                                new Object[] { (byte) (j + (i - 1) * 3), arr[j - 1] + i });
                    }
                }
                Assert.assertFalse(rs.next());
            }

            s.execute("set flatten_nested=1; drop table if exists test_nested_insert; "
                    + "create table test_nested_insert(id UInt32, n Nested(c1 Int8, c2 LowCardinality(String)))engine=Memory");
            try (PreparedStatement ps = conn.prepareStatement(
                    "insert into test_nested_insert(id, n.c1, n.c2) select id, c1, c2 from input('id UInt32, c1 Array(Int8), c2 Array(LowCardinality(String))')")) {
                ps.setString(1, "3");
                ps.setObject(2, new byte[] { 7, 8, 9 });
                ps.setObject(3, new String[] { "foo3", "bar3", "bug3" });
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("select * from test_nested_insert order by id")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getObject(2), new byte[] { 7, 8, 9 });
                Assert.assertEquals(rs.getObject(3), new String[] { "foo3", "bar3", "bug3" });
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(dataProvider = "columnsWithoutDefaultValue", groups = "integration")
    public void testInsertNullValue(String columnType, String defaultValue) throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.FORMAT.getKey(),
                ClickHouseFormat.TabSeparatedWithNamesAndTypes.name());
        props.setProperty(ClickHouseClientOption.CUSTOM_SETTINGS.getKey(), "input_format_null_as_default=0");
        String tableName = "test_insert_null_value_" + columnType.split("\\(")[0].trim().toLowerCase();
        try (ClickHouseConnection conn = newConnection(props); Statement s = conn.createStatement()) {
            if (conn.getUri().toString().contains(":grpc:")) {
                throw new SkipException("Skip gRPC test");
            } else if (!conn.getServerVersion().check("[22.3,)")) {
                throw new SkipException("Skip test when ClickHouse is older than 22.3");
            }
            s.execute(String.format("drop table if exists %s; ", tableName)
                    + String.format("create table %s(id Int8, v %s)engine=Memory", tableName, columnType));
            SQLException sqlException = null;
            try (PreparedStatement stmt = conn
                    .prepareStatement(String.format("insert into %s values(?,?)", tableName))) {
                try {
                    ((ClickHouseStatement) stmt).setNullAsDefault(0);
                    stmt.setInt(1, 0);
                    stmt.setObject(2, null);
                    stmt.executeUpdate();
                } catch (SQLException e) {
                    sqlException = e;
                }
                Assert.assertNotNull(sqlException, "Should end-up with SQL exception when nullAsDefault < 1");
                sqlException = null;

                try {
                    ((ClickHouseStatement) stmt).setNullAsDefault(1);
                    stmt.setInt(1, 0);
                    stmt.setNull(2, Types.OTHER);
                    stmt.executeUpdate();
                } catch (SQLException e) {
                    sqlException = e;
                }
                Assert.assertNotNull(sqlException, "Should end-up with SQL exception when nullAsDefault = 1");

                ((ClickHouseStatement) stmt).setNullAsDefault(2);
                stmt.setInt(1, 1);
                stmt.setObject(2, null);
                stmt.executeUpdate();
                stmt.setInt(1, 2);
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
                Assert.assertFalse(rs.next(), "Should have only 2 rows");
            }
            Assert.assertEquals(rowCount, 2);
        } catch (SQLException e) {
            // 'Unknown data type family', 'Missing columns' or 'Cannot create table column'
            if (e.getErrorCode() == 50 || e.getErrorCode() == 47 || e.getErrorCode() == 44) {
                return;
            }
            throw e;
        }
    }

    @Test(groups = "integration")
    public void testInsertStringAsArray() throws SQLException {
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
            Assert.assertEquals(rs.getObject(3),
                    new UnsignedInteger[] { UnsignedInteger.valueOf(3), null, UnsignedInteger.ONE });
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
    public void testInsertWithFunction() throws SQLException, UnknownHostException {
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
    public void testInsertWithSelect() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement();
                PreparedStatement ps1 = conn
                        .prepareStatement("insert into test_issue_402(uid,uuid) select 2,generateUUIDv4()");
                PreparedStatement ps2 = conn.prepareStatement(
                        "insert into test_issue_402\nselect ?, max(uuid) from test_issue_402 where uid in (?) group by uid having count(*) = 1")) {
            s.execute("drop table if exists test_issue_402; "
                    + "create table test_issue_402(uid Int32, uuid UUID)engine=Memory");
            Assert.assertEquals(ps1.executeUpdate(), 1);
            ps2.setInt(1, 1);
            ps2.setInt(2, 2);
            Assert.assertEquals(ps2.executeUpdate(), 1);

            ResultSet rs = s.executeQuery("select * from test_issue_402 order by uid");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            String uuid = rs.getString(2);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getString(2), uuid);
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
    public void testInsertWithAndSelect() throws SQLException {
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

    @Test(groups = "integration")
    public void testInsertWithMultipleValues() throws MalformedURLException, SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_insert_with_multiple_values; "
                    + "CREATE TABLE test_insert_with_multiple_values(a Int32, b Nullable(String)) ENGINE=Memory");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO test_insert_with_multiple_values values(?, ?), (2 , ? ), ( ? , '') , (?,?) ,( ? ,? )")) {
                ps.setInt(1, 1);
                ps.setNull(2, Types.VARCHAR);
                ps.setObject(3, "er");
                ps.setInt(4, 3);
                ps.setInt(5, 4);
                ps.setURL(6, new URL("http://some.host"));
                ps.setInt(7, 5);
                ps.setString(8, null);
                ps.executeUpdate();
            }

            try (ResultSet rs = s.executeQuery("select * from test_insert_with_multiple_values order by a")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getByte(1), (byte) 1);
                Assert.assertEquals(rs.getObject(2), null);
                Assert.assertTrue(rs.wasNull());
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getBigDecimal(1), BigDecimal.valueOf(2L));
                Assert.assertEquals(rs.getString(2), "er");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "3");
                Assert.assertEquals(rs.getObject(2), "");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getShort(1), (short) 4);
                Assert.assertEquals(rs.getURL(2), new URL("http://some.host"));
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getObject(1), Integer.valueOf(5));
                Assert.assertEquals(rs.getString(2), null);
                Assert.assertTrue(rs.wasNull());
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration")
    public void testInsertWithNullDateTime() throws SQLException {
        Properties props = new Properties();
        props.setProperty(JdbcConfig.PROP_NULL_AS_DEFAULT, "2");
        try (ClickHouseConnection conn = newConnection(props);
                Statement s = conn.createStatement()) {
            s.execute("drop table if exists test_insert_with_null_datetime; "
                    + "CREATE TABLE test_insert_with_null_datetime(a Int32, "
                    + "b01 DateTime32, b02 DateTime32('America/Los_Angeles'), "
                    + "b11 DateTime32, b12 DateTime32('America/Los_Angeles'), "
                    + "c01 DateTime64(3), c02 DateTime64(6, 'Asia/Shanghai'), "
                    + "c11 DateTime64(3), c12 DateTime64(6, 'Asia/Shanghai')) ENGINE=Memory");
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO test_insert_with_null_datetime values(?, ? ,? ,?,?)")) {
                ps.setInt(1, 1);
                ps.setObject(2, LocalDateTime.now());
                ps.setObject(3, LocalDateTime.now());
                ps.setTimestamp(4, null);
                ps.setNull(5, Types.TIMESTAMP);
                ps.setObject(6, LocalDateTime.now());
                ps.setObject(7, LocalDateTime.now());
                ps.setObject(8, null);
                ps.setTimestamp(9, null, Calendar.getInstance());
                ps.executeUpdate();
            }

            try (ResultSet rs = s.executeQuery("select * from test_insert_with_null_datetime order by a")) {
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration")
    public void testInsertWithFormat() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseHttpOption.WAIT_END_OF_QUERY.getKey(), "true");
        try (ClickHouseConnection conn = newConnection(props); Statement s = conn.createStatement()) {
            if (!conn.getServerVersion().check("[22.5,)")) {
                throw new SkipException(
                        "Skip due to breaking change introduced by https://github.com/ClickHouse/ClickHouse/pull/35883");
            }

            s.execute("drop table if exists test_insert_with_format; "
                    + "CREATE TABLE test_insert_with_format(i Int32, s String) ENGINE=MergeTree ORDER BY i");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test_insert_with_format format CSV")) {
                Assert.assertTrue(ps instanceof StreamBasedPreparedStatement);
                Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 1);
                Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(1), String.class.getName());
                ps.setObject(1, ClickHouseInputStream.of("1,\\N\n2,two"));
                ps.executeUpdate();
            }

            try (ResultSet rs = s.executeQuery("SELECT * FROM test_insert_with_format ORDER BY i" + (isCloud() ? " SETTINGS select_sequential_consistency=1" : ""))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(rs.getString(2), "two");
                Assert.assertFalse(rs.next());
            }

            s.execute("truncate table test_insert_with_format");

            try (PreparedStatement ps = conn
                    .prepareStatement(
                            "INSERT INTO test_insert_with_format(s,i)SETTINGS insert_null_as_default=1 format JSONEachRow")) {
                Assert.assertTrue(ps instanceof StreamBasedPreparedStatement);
                ps.setString(1, "{\"i\":null,\"s\":null}");
                ps.addBatch();
                ps.setObject(1, "{\"i\":1,\"s\":\"one\"}");
                ps.addBatch();
                ps.setObject(1, new ClickHouseWriter() {
                    @Override
                    public void write(ClickHouseOutputStream out) throws IOException {
                        out.write("{\"i\":2,\"s\":\"22\"}".getBytes());
                    }
                });
                ps.addBatch();
                Assert.assertEquals(ps.executeBatch(), new int[] { 1, 1, 1 });
            }

            try (PreparedStatement ps = conn
                    .prepareStatement(
                            "INSERT INTO test_insert_with_format(s,i) select * from input('s String, i Int32') format CSV")) {
                Assert.assertFalse(ps instanceof StreamBasedPreparedStatement);
                ps.setInt(2, 3);
                ps.setString(1, "three");
                Assert.assertEquals(ps.executeUpdate(), 1);
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "select i,s from test_insert_with_format order by i format RowBinaryWithNamesAndTypes");
                    ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 0);
                Assert.assertEquals(rs.getString(2), "");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "one");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(rs.getString(2), "22");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 3);
                Assert.assertEquals(rs.getString(2), "three");
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "integration")
    public void testInsertWithSettings() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props); Statement s = conn.createStatement()) {
            if (!conn.getServerVersion().check("[22.5,)")) {
                throw new SkipException(
                        "Skip due to breaking change introduced by https://github.com/ClickHouse/ClickHouse/pull/35883");
            }

            s.execute("drop table if exists test_insert_with_settings; "
                    + "CREATE TABLE test_insert_with_settings(i Int32, s String) ENGINE=Memory");
            try (PreparedStatement ps = conn
                    .prepareStatement(
                            "INSERT INTO test_insert_with_settings SETTINGS async_insert=1,wait_for_async_insert=1 values(?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "1");
                ps.addBatch();
                ps.executeBatch();
            }

            try (ResultSet rs = s.executeQuery("select * from test_insert_with_settings order by i")) {
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
            }
        }
    }

    //TODO: This test is failing both on cloud and locally, need to investigate
    @Test(groups = "integration", enabled = false)
    public void testGetMetadataTypes() throws SQLException {
        try (Connection conn = newConnection(new Properties());
            PreparedStatement ps = conn.prepareStatement("select ? a, ? b")) {
            ResultSetMetaData md = ps.getMetaData();
            Assert.assertEquals(md.getColumnCount(), 2);
            Assert.assertEquals(md.getColumnName(1), "a");
            Assert.assertEquals(md.getColumnTypeName(1), "Nullable(Nothing)");
            Assert.assertEquals(md.getColumnName(2), "b");
            Assert.assertEquals(md.getColumnTypeName(2), "Nullable(Nothing)");

            ps.setString(1, "x");
            md = ps.getMetaData();
            Assert.assertEquals(md.getColumnCount(), 2);
            Assert.assertEquals(md.getColumnName(1), "a");
            Assert.assertEquals(md.getColumnTypeName(1), "String");
            Assert.assertEquals(md.getColumnName(2), "b");
            Assert.assertEquals(md.getColumnTypeName(2), "Nullable(Nothing)");

            ps.setObject(2, new BigInteger("12345"));
            md = ps.getMetaData();
            Assert.assertEquals(md.getColumnCount(), 2);
            Assert.assertEquals(md.getColumnName(1), "a");
            Assert.assertEquals(md.getColumnTypeName(1), "String");
            Assert.assertEquals(md.getColumnName(2), "b");
            Assert.assertEquals(md.getColumnTypeName(2), "UInt16");

            ps.addBatch();
            ps.setInt(1, 2);
            md = ps.getMetaData();
            Assert.assertEquals(md.getColumnCount(), 2);
            Assert.assertEquals(md.getColumnName(1), "a");
            Assert.assertEquals(md.getColumnTypeName(1), "String");
            Assert.assertEquals(md.getColumnName(2), "b");
            Assert.assertEquals(md.getColumnTypeName(2), "UInt16");

            ps.clearBatch();
            ps.clearParameters();
            md = ps.getMetaData();
            Assert.assertEquals(md.getColumnCount(), 2);
            Assert.assertEquals(md.getColumnName(1), "a");
            Assert.assertEquals(md.getColumnTypeName(1), "Nullable(Nothing)");
            Assert.assertEquals(md.getColumnName(2), "b");
            Assert.assertEquals(md.getColumnTypeName(2), "Nullable(Nothing)");
        }
    }

    @Test(groups = "integration", enabled = false)
    public void testGetMetadataStatements() throws SQLException {
        if (isCloud()) return; //TODO: testGetMetadataStatements - Skipping because it doesn't seem valid, we should revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        try (Connection conn = newConnection(new Properties());
            PreparedStatement createPs = conn.prepareStatement("create table test_get_metadata_statements (col String) Engine=Log");
            PreparedStatement selectPs = conn.prepareStatement("select 'Hello, World!'");
            PreparedStatement insertPs = conn.prepareStatement(
                "insert into test_get_metadata_statements select 'Hello, World!'");
            PreparedStatement updatePs = conn.prepareStatement(
                "update test_get_metadata_statements set col = 'Bye, World!'");
            PreparedStatement grantPs = conn.prepareStatement("grant select on * to default");
            PreparedStatement commitPS = conn.prepareStatement("commit");) {

            // Only select shall have valid metadata
            ResultSetMetaData selectMetaData = selectPs.getMetaData();
            Assert.assertNotNull(selectMetaData);
            Assert.assertEquals(selectMetaData.getColumnCount(), 1);
            Assert.assertEquals(selectMetaData.getColumnTypeName(1), "String");

            // The rest shall return null
            Assert.assertNull(createPs.getMetaData());
            Assert.assertNull(insertPs.getMetaData());
            Assert.assertNull(updatePs.getMetaData());
            Assert.assertNull(grantPs.getMetaData());
            Assert.assertNull(commitPS.getMetaData());
        }
    }

    @Test(groups = "integration")
    public void testGetParameterMetaData() throws SQLException {
        try (Connection conn = newConnection(new Properties());
                PreparedStatement emptyPs = conn.prepareStatement("select 1");
                PreparedStatement inputPs = conn.prepareStatement(
                        "insert into non_existing_table select * from input('col2 String, col3 Int8, col1 JSON')");
                PreparedStatement sqlPs = conn.prepareStatement("select ?, toInt32(?), ? b");
                PreparedStatement tablePs = conn.prepareStatement(
                        "select a.id, c.* from {tt 'col2'} a inner join {tt 'col3'} b on a.id = b.id left outer join {tt 'col1'} c on b.id = c.id");) {
            Assert.assertEquals(emptyPs.getParameterMetaData().getParameterCount(), 0);

            for (PreparedStatement ps : new PreparedStatement[] { inputPs, sqlPs }) {
                Assert.assertNotNull(ps.getParameterMetaData());
                Assert.assertTrue(ps.getParameterMetaData() == ps.getParameterMetaData(),
                        "parameter mete data should be singleton");
                Assert.assertEquals(ps.getParameterMetaData().getParameterCount(), 3);
                Assert.assertEquals(ps.getParameterMetaData().getParameterMode(3), ParameterMetaData.parameterModeIn);
                Assert.assertEquals(ps.getParameterMetaData().getParameterType(3), Types.VARCHAR);
                Assert.assertEquals(ps.getParameterMetaData().getPrecision(3), 0);
                Assert.assertEquals(ps.getParameterMetaData().getScale(3), 0);
                Assert.assertEquals(ps.getParameterMetaData().getParameterClassName(3), Object.class.getName());
                Assert.assertEquals(ps.getParameterMetaData().getParameterTypeName(3), ClickHouseDataType.JSON.name());
            }
        }
    }
}
