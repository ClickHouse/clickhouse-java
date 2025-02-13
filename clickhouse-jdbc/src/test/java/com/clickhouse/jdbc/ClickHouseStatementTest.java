package com.clickhouse.jdbc;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseParameterizedQuery;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseBitmap;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedLong;
import com.clickhouse.data.value.UnsignedShort;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ClickHouseStatementTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @DataProvider(name = "timeZoneTestOptions")
    private Object[][] getTimeZoneTestOptions() {
        return new Object[][] {
                new Object[] { true }, new Object[] { false } };
    }

    @DataProvider(name = "connectionProperties")
    private Object[][] getConnectionProperties() {
        Properties emptyProps = new Properties();
        Properties sessionProps = new Properties();
        sessionProps.setProperty(ClickHouseClientOption.SESSION_ID.getKey(), UUID.randomUUID().toString());
        return new Object[][] {
                new Object[] { emptyProps }, new Object[] { sessionProps } };
    }

    @Test(groups = "integration")
    public void testBatchUpdate() throws SQLException {
        if (isCloud()) return; //TODO: testBatchUpdate - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props); ClickHouseStatement stmt = conn.createStatement()) {
            if (!conn.getServerVersion().check("[22.8,)")) {
                throw new SkipException("Skip due to error 'unknown key zookeeper_load_balancing'");
            }

            stmt.addBatch("drop table if exists test_batch_dll_on_cluster on cluster single_node_cluster_localhost");
            stmt.addBatch(
                    "create table if not exists test_batch_dll_on_cluster on cluster single_node_cluster_localhost(a Int64) Engine=MergeTree order by a;"
                            + "drop table if exists test_batch_dll_on_cluster on cluster single_node_cluster_localhost;");
            Assert.assertEquals(stmt.executeBatch(), new int[] { 0, 0, 0 });

            stmt.addBatch("drop table if exists test_batch_queries");
            stmt.addBatch("select 1");
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
        }
    }

    @Test(groups = "integration")
    public void testBitmap64() throws SQLException {
        Properties props = new Properties();
        String sql = "select k,\n"
                + "[ tuple(arraySort(groupUniqArrayIf(n, n > 33)), groupBitmapStateIf(n, n > 33)),\n"
                + "  tuple(arraySort(groupUniqArrayIf(n, n < 32)), groupBitmapStateIf(n, n < 32)),\n"
                + "  tuple(arraySort(groupUniqArray(n)), groupBitmapState(n)),\n"
                + "  tuple(arraySort(groupUniqArray(v)), groupBitmapState(v))\n"
                + "]::Array(Tuple(Array(UInt64), AggregateFunction(groupBitmap, UInt64))) v\n"
                + "from (select 'k' k, (number % 33)::UInt64 as n, (9223372036854775807 + number::Int16)::UInt64 v from numbers(300000))\n"
                + "group by k";
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists test_bitmap64_serde; "
                    + "create table test_bitmap64_serde(k String, v Array(Tuple(Array(UInt64), AggregateFunction(groupBitmap, UInt64))))engine=Memory");
            try (PreparedStatement ps = conn.prepareStatement("insert into test_bitmap64_serde");
                    ResultSet rs = stmt.executeQuery(sql)) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString(1), "k");
                ps.setString(1, rs.getString(1));
                Object[] values = (Object[]) rs.getObject(2);
                ps.setObject(2, values);
                Assert.assertEquals(values.length, 4);
                for (int i = 0; i < values.length; i++) {
                    List<?> tuple = (List<?>) values[i];
                    Assert.assertEquals(tuple.size(), 2);
                    long[] nums = (long[]) tuple.get(0);
                    ClickHouseBitmap bitmap = (ClickHouseBitmap) tuple.get(1);
                    Roaring64NavigableMap bitmap64 = (Roaring64NavigableMap) bitmap.unwrap();
                    Assert.assertEquals(nums.length, bitmap64.getLongCardinality());
                    for (int j = 0; j < nums.length; j++) {
                        Assert.assertTrue(bitmap64.contains(nums[j]), "Bitmap does not contain value: " + nums[j]);
                    }
                }
                Assert.assertFalse(rs.next());

                // Assert.assertThrows(IllegalStateException.class, () -> ps.executeUpdate());
                Roaring64NavigableMap.SERIALIZATION_MODE = Roaring64NavigableMap.SERIALIZATION_MODE_PORTABLE;
                ps.executeUpdate();
            }

            stmt.execute("insert into test_bitmap64_serde\n" + sql);
            try (ResultSet rs = stmt.executeQuery("select distinct * from test_bitmap64_serde")) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertFalse(rs.next(), "Should have only one unique row");
            }
        }
    }

    @Test(groups = "integration")
    public void testDialect() throws SQLException {
        Properties props = new Properties();
        String sql = "select cast(1 as UInt64) a, cast([1, 2] as Array(Int8)) b";
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getMetaData().getColumnTypeName(1), ClickHouseDataType.UInt64.name());
            Assert.assertEquals(rs.getMetaData().getColumnTypeName(2), "Array(Int8)");
            Assert.assertFalse(rs.next());
        }

        props.setProperty(JdbcConfig.PROP_DIALECT, "ansi");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql);) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getMetaData().getColumnTypeName(1), "DECIMAL(20,0)");
            Assert.assertEquals(rs.getMetaData().getColumnTypeName(2), "ARRAY(BYTE)");
            Assert.assertFalse(rs.next());
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
        File f1 = new File("a1.csv");
        if (f1.exists()) {
            f1.delete();
        }
        f1.deleteOnExit();
        File f2 = new File("a2.csv");
        if (f2.exists()) {
            f2.delete();
        }
        f2.deleteOnExit();

        try (ClickHouseConnection conn = newConnection(props)) {
            String sql1 = "SELECT number n, toString(n) FROM numbers(1234) into outfile '" + f1.getName() + "'";
            try (ClickHouseStatement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql1)) {
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
                Assert.assertTrue(f1.exists());
                // end up with exception because the file already exists
                Assert.assertThrows(SQLException.class, () -> stmt.executeQuery(sql1));
            }

            // try again with ! suffix to override the existing file
            String sql = "select number n, toString(n) from numbers(1234) into outfile '" + f1.getName() + "!'";
            try (ClickHouseStatement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
                Assert.assertTrue(f1.exists());
            }

            sql = "select number n, toString(n) from numbers(4321) into outfile '" + f2.getName() + "!'";
            try (ClickHouseStatement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {
                Assert.assertTrue(rs.next());
                Assert.assertFalse(rs.next());
            }

            try (ClickHouseStatement stmt = conn.createStatement()) {
                Assert.assertFalse(stmt.execute(
                        "drop table if exists test_load_infile; create table test_load_infile(n UInt64, s String)engine=Memory"));
                stmt.executeUpdate("insert into test_load_infile from infile 'a?.csv'");
                try (ResultSet rs = stmt.executeQuery("select count(1) from test_load_infile")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), 5555);
                    Assert.assertFalse(rs.next());
                }
            }

            try (ClickHouseStatement stmt = conn.createStatement()) {
                // it's fine when no record was inserted
                stmt.executeUpdate("insert into test_load_infile from infile 'non-existent.csv'");
                // unless suffix ! was added...
                Assert.assertThrows(SQLException.class,
                        () -> stmt.executeUpdate("insert into test_load_infile from infile 'non-existent.csv!'"));
                try (ResultSet rs = stmt.executeQuery("select count(1) from test_load_infile")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getInt(1), 5555);
                    Assert.assertFalse(rs.next());
                }
            }
        }
    }

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
    public void testSocketTimeout() throws SQLException {
        Properties props = new Properties();
        props.setProperty("connect_timeout", "500");
        props.setProperty("socket_timeout", "1000");
        props.setProperty("database", "system");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            if (stmt.unwrap(ClickHouseRequest.class).getServer().getProtocol() != ClickHouseProtocol.HTTP) {
                throw new SkipException("Skip as only http implementation works well");
            }
            stmt.executeQuery("select sleep(3)");
            Assert.fail("Should throw timeout exception");
        } catch (SQLException e) {
            Assert.assertTrue(e.getCause() instanceof java.net.SocketTimeoutException
                    || e.getCause() instanceof IOException,
                    "Should throw SocketTimeoutException or HttpTimeoutException");
        }
    }

    @Test(groups = "integration")
    public void testSwitchCatalog() throws SQLException {
        if (isCloud()) return; //TODO: testSwitchCatalog - Revisit, see:https://github.com/ClickHouse/clickhouse-java/issues/1747
        Properties props = new Properties();
        props.setProperty("databaseTerm", "catalog");
        props.setProperty("database", "system");
        String dbName = "test_switch_schema";
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            Assert.assertEquals(conn.getCatalog(), "system");
            Assert.assertEquals(conn.getSchema(), null);
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
            conn.setCatalog(dbName);
            conn.setSchema("non-existent-catalog");
            Assert.assertEquals(conn.getCatalog(), dbName);
            Assert.assertEquals(conn.getSchema(), null);
            rs = stmt.executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), dbName);
            Assert.assertFalse(rs.next());

            conn.createStatement().execute("use system");
            Assert.assertEquals(conn.getCurrentDatabase(), "system");
            Assert.assertEquals(conn.getCatalog(), "system");
            Assert.assertEquals(conn.getSchema(), null);
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertFalse(rs.next());

            conn.createStatement().execute("use `" + dbName + "`");
            Assert.assertEquals(conn.getCurrentDatabase(), dbName);
            Assert.assertEquals(conn.getCatalog(), dbName);
            Assert.assertEquals(conn.getSchema(), null);
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), dbName);
            Assert.assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("use system;select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertEquals(conn.getCatalog(), "system");
            Assert.assertEquals(conn.getSchema(), null);
            Assert.assertFalse(rs.next());

            // non-existent databse
            String nonExistentDb = UUID.randomUUID().toString();
            Assert.assertThrows(SQLException.class, () -> conn.setCatalog(nonExistentDb));
            Assert.assertThrows(SQLException.class, () -> conn.createStatement().execute("use an invalid query"));
            Assert.assertThrows(SQLException.class,
                    () -> conn.createStatement().execute("use `" + nonExistentDb + "`"));
            Assert.assertThrows(SQLException.class,
                    () -> conn.createStatement().execute("use `" + nonExistentDb + "`; select 1"));
        } finally {
            dropDatabase(dbName);
        }
    }

    @Test(groups = "integration")
    public void testSwitchSchema() throws SQLException {
        if (isCloud()) return; //TODO: testSwitchSchema - Revisit, see:https://github.com/ClickHouse/clickhouse-java/issues/1747
        Properties props = new Properties();
        props.setProperty("databaseTerm", "schema");
        props.setProperty("database", "system");
        String dbName = "test_switch_schema";
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            Assert.assertEquals(conn.getCatalog(), null);
            Assert.assertEquals(conn.getSchema(), "system");
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
            conn.setCatalog("non-existent-catalog");
            conn.setSchema(dbName);
            Assert.assertEquals(conn.getCatalog(), null);
            Assert.assertEquals(conn.getSchema(), dbName);
            rs = stmt.executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertFalse(rs.next());
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), dbName);
            Assert.assertFalse(rs.next());

            conn.createStatement().execute("use system");
            Assert.assertEquals(conn.getCurrentDatabase(), "system");
            Assert.assertEquals(conn.getCatalog(), null);
            Assert.assertEquals(conn.getSchema(), "system");
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertFalse(rs.next());

            conn.createStatement().execute("use `" + dbName + "`");
            Assert.assertEquals(conn.getCurrentDatabase(), dbName);
            Assert.assertEquals(conn.getCatalog(), null);
            Assert.assertEquals(conn.getSchema(), dbName);
            rs = conn.createStatement().executeQuery("select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), dbName);
            Assert.assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("use system;select currentDatabase()");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "system");
            Assert.assertEquals(conn.getCatalog(), null);
            Assert.assertEquals(conn.getSchema(), "system");
            Assert.assertFalse(rs.next());

            // non-existent databse
            String nonExistentDb = UUID.randomUUID().toString();
            Assert.assertThrows(SQLException.class, () -> conn.setSchema(nonExistentDb));
            Assert.assertThrows(SQLException.class, () -> conn.createStatement().execute("use an invalid query"));
            Assert.assertThrows(SQLException.class,
                    () -> conn.createStatement().execute("use `" + nonExistentDb + "`"));
            Assert.assertThrows(SQLException.class,
                    () -> conn.createStatement().execute("use `" + nonExistentDb + "`; select 1"));
        } finally {
            dropDatabase(dbName);
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
                    "select distinct query from system.query_log where type = 'QueryStart' and log_comment = 'select something "
                            + uuid + "'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), sql);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testMaxFloatValues() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement s = conn.createStatement()) {
            s.execute("drop table if exists test_float_values; "
                    + "create table test_float_values(f1 Nullable(Float64), f2 Nullable(Float64))engine=Memory");
            try (PreparedStatement ps = conn.prepareStatement("insert into test_float_values values(?, ?)")) {
                ps.setObject(1, Float.MAX_VALUE);
                ps.setObject(2, Double.MAX_VALUE);
                ps.executeUpdate();
            }
            ResultSet rs = s.executeQuery("select * from test_float_values");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getFloat(1), Float.MAX_VALUE);
            Assert.assertEquals(rs.getDouble(2), Double.MAX_VALUE);
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testMutation() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props); ClickHouseStatement stmt = conn.createStatement()) {
            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);

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
            Assert.assertEquals(conn.createStatement().executeUpdate("update test_mutation set b = 22 where b = 1"), 1);

            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeUpdate("update non_existing_table set value=1 where key=1"));

            stmt.addBatch("insert into test_mutation values('1',1)");
            stmt.addBatch("drop table non_existing_table");
            stmt.addBatch("insert into test_mutation values('2',2)");
            Assert.assertThrows(SQLException.class, () -> stmt.executeBatch());

            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);
        }

        props.setProperty(JdbcConfig.PROP_CONTINUE_BATCH, "true");
        try (ClickHouseConnection conn = newConnection(props); ClickHouseStatement stmt = conn.createStatement()) {
            stmt.addBatch("insert into test_mutation values('a',1)");
            stmt.addBatch("drop table non_existing_table");
            stmt.addBatch("insert into test_mutation values('b',2)");
            stmt.addBatch("select 2");
            Assert.assertEquals(stmt.executeBatch(),
                    new int[] { 1, Statement.EXECUTE_FAILED, 1, Statement.EXECUTE_FAILED });
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
                    + "CREATE TABLE test_async_insert(id UInt32, s String) ENGINE = MergeTree ORDER BY id; "
                    + "INSERT INTO test_async_insert VALUES(1, 'a'); "
                    + "SELECT * FROM test_async_insert" + (isCloud() ? " SETTINGS select_sequential_consistency=1" : ""));
            ResultSet rs = stmt.getResultSet();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getString(2), "a");
            Assert.assertFalse(rs.next());
        }

        //TODO: I'm not sure this is a valid test...
        if (isCloud()) return; //TODO: testAsyncInsert - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        props.setProperty(ClickHouseHttpOption.CUSTOM_PARAMS.getKey(), "async_insert=1,wait_for_async_insert=0");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("TRUNCATE TABLE test_async_insert; "
                    + "INSERT INTO test_async_insert VALUES(1, 'a'); "
                    + "SELECT * FROM test_async_insert");
            ResultSet rs = stmt.getResultSet();
            Assert.assertFalse(rs.next(),
                    "Server was probably busy at that time, so the row was inserted before your query");
        }
    }

    @Test(dataProvider = "connectionProperties", groups = "integration")
    public void testCancelQuery(Properties props) throws SQLException {
        if (isCloud()) return; //TODO: testCancelQuery - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();) {
            CountDownLatch c = new CountDownLatch(1);
            ClickHouseClient.submit(() -> stmt.executeQuery("select * from numbers(100000000)")).whenComplete(
                    (rs, e) -> {
                        Assert.assertNull(e, "Should NOT have any exception");

                        int index = 0;

                        try {
                            while (rs.next()) {
                                if (index++ < 1) {
                                    c.countDown();
                                }
                            }
                            Assert.fail("Query should have been cancelled");
                        } catch (SQLException ex) {
                            Assert.assertNotNull(ex, "Should end up with exception");
                        }
                    });
            try {
                c.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                Assert.fail("Failed to wait", e);
            } finally {
                stmt.cancel();
            }

            try (ResultSet rs = stmt.executeQuery("select 5")) {
                Assert.assertTrue(rs.next(), "Should have at least one record");
                Assert.assertEquals(rs.getInt(1), 5);
                Assert.assertFalse(rs.next(), "Should have only one record");
            }
        }
    }

    @Test(groups = "integration")
    public void testExecute() throws SQLException {
        try (Connection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            // ddl
            Assert.assertFalse(stmt.execute("drop table if exists non_existing_table"), "Should have no result set");
            Assert.assertEquals(stmt.getResultSet(), null);
            Assert.assertTrue(stmt.getUpdateCount() >= 0, "Should have update count");
            // query
            Assert.assertTrue(stmt.execute("select 1"), "Should have result set");
            ResultSet rs = stmt.getResultSet();
            Assert.assertTrue(rs.next(), "Should have one record");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one record");
            // mixed usage
            stmt.addBatch("drop table if exists non_existing_table");
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("drop table if exists non_existing_table"));
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select 2"));
            stmt.clearBatch();
            Assert.assertFalse(stmt.execute("drop table if exists non_existing_table"), "Should have no result set");
            Assert.assertEquals(stmt.getResultSet(), null);
            Assert.assertTrue(stmt.getUpdateCount() >= 0, "Should have update count");
            Assert.assertTrue(stmt.execute("select 2"), "Should have result set");
            rs = stmt.getResultSet();
            Assert.assertTrue(rs.next(), "Should have one record");
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertFalse(rs.next(), "Should have only one record");
        }
    }

    @Test(groups = "integration")
    public void testExecuteBatch() throws SQLException {
        Properties props = new Properties();
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);
            stmt.addBatch("select 1");
            stmt.clearBatch();
            Assert.assertEquals(stmt.executeBatch(), new int[0]);
            Assert.assertEquals(stmt.executeLargeBatch(), new long[0]);
            stmt.addBatch("select 1");
            // mixed usage
            Assert.assertThrows(SQLException.class, () -> stmt.execute("select 2"));
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select 2"));
            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeLargeUpdate("drop table if exists non_existing_table"));
            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeUpdate("drop table if exists non_existing_table"));
            // query in batch
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());
            stmt.addBatch("select 1");
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeLargeBatch());

            Assert.assertFalse(stmt.execute("drop table if exists test_execute_batch; "
                    + "create table test_execute_batch(a Int32, b String)engine=Memory"), "Should not have result set");
            stmt.addBatch("insert into test_execute_batch values(1,'1')");
            stmt.addBatch("insert into test_execute_batch values(2,'2')");
            stmt.addBatch("insert into test_execute_batch values(3,'3')");
            Assert.assertEquals(stmt.executeBatch(), new int[] { 1, 1, 1 });

            Assert.assertFalse(stmt.execute("truncate table test_execute_batch"), "Should not have result set");
            stmt.addBatch("insert into test_execute_batch values(1,'1')");
            stmt.addBatch("insert into test_execute_batch values(2,'2')");
            stmt.addBatch("insert into test_execute_batch values(3,'3')");
            Assert.assertEquals(stmt.executeLargeBatch(), new long[] { 1L, 1L, 1L });

            try (ResultSet rs = stmt.executeQuery("select * from test_execute_batch order by a")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    Assert.assertEquals(rs.getInt(1), count);
                    Assert.assertEquals(rs.getString(2), String.valueOf(count));
                }
                Assert.assertEquals(count, 3);
            }

            Assert.assertFalse(stmt.execute("truncate table test_execute_batch"), "Should not have result set");
            stmt.addBatch("insert into test_execute_batch values(1,'1')");
            stmt.addBatch("drop table non_existing_table");
            stmt.addBatch("insert into test_execute_batch values(2,'2')");
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

            Assert.assertFalse(stmt.execute("truncate table test_execute_batch"), "Should not have result set");
            stmt.addBatch("insert into test_execute_batch values(1,'1')");
            stmt.addBatch("drop table non_existing_table");
            stmt.addBatch("insert into test_execute_batch values(2,'2')");
            Assert.assertThrows(BatchUpdateException.class, () -> stmt.executeLargeBatch());
        }

        props.setProperty(JdbcConfig.PROP_CONTINUE_BATCH, "true");
        try (Connection conn = newConnection(props); Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute("truncate table test_execute_batch"), "Should not have result set");
            stmt.addBatch("insert into test_execute_batch values(1,'1')");
            stmt.addBatch("drop table non_existing_table");
            stmt.addBatch("insert into test_execute_batch values(2,'2')");
            stmt.addBatch("drop table non_existing_table");
            Assert.assertEquals(stmt.executeBatch(),
                    new int[] { 1, Statement.EXECUTE_FAILED, 1, Statement.EXECUTE_FAILED });

            Assert.assertFalse(stmt.execute("truncate table test_execute_batch"), "Should not have result set");
            stmt.addBatch("insert into test_execute_batch values(1,'1')");
            stmt.addBatch("drop table non_existing_table");
            stmt.addBatch("insert into test_execute_batch values(2,'2')");
            stmt.addBatch("drop table non_existing_table");
            Assert.assertEquals(stmt.executeLargeBatch(),
                    new long[] { 1L, Statement.EXECUTE_FAILED, 1L, Statement.EXECUTE_FAILED });
            try (ResultSet rs = stmt.executeQuery("select * from test_execute_batch order by a")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    Assert.assertEquals(rs.getInt(1), count);
                    Assert.assertEquals(rs.getString(2), String.valueOf(count));
                }
                Assert.assertEquals(count, 2);
            }
        }
    }

    @Test(groups = "integration")
    public void testExecuteQuery() throws SQLException {
        try (Connection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select 1");
            Assert.assertTrue(rs == stmt.getResultSet(), "Should be the exact same result set");
            Assert.assertEquals(stmt.getUpdateCount(), -1);
            Assert.assertTrue(rs.next(), "Should have one record");
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertFalse(rs.next(), "Should have only one record");

            stmt.addBatch("select 1");
            Assert.assertThrows(SQLException.class, () -> stmt.executeQuery("select 2"));
            stmt.clearBatch();
            rs = stmt.executeQuery("select 2");
            Assert.assertTrue(rs == stmt.getResultSet(), "Should be the exact same result set");
            Assert.assertEquals(stmt.getUpdateCount(), -1);
            Assert.assertTrue(rs.next(), "Should have one record");
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertFalse(rs.next(), "Should have only one record");

            // never return null result set
            rs = stmt.executeQuery("drop table if exists non_existing_table");
            Assert.assertNotNull(rs, "Should never be null");
            Assert.assertNull(stmt.getResultSet(), "Should be null");
            Assert.assertEquals(stmt.getUpdateCount(), 0);
            Assert.assertFalse(rs.next(), "Should has no row");
        }
    }

    @Test(groups = "integration")
    public void testExecuteUpdate() throws SQLException {
        try (Connection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute("drop table if exists test_execute_query; "
                    + "create table test_execute_query(a Int32, b String)engine=Memory"), "Should not have result set");

            Assert.assertTrue(stmt.executeUpdate("insert into test_execute_query values(1,'1')") >= 0,
                    "Should return value greater than or equal to zero");
            Assert.assertNull(stmt.getResultSet(), "Should have no result set");
            Assert.assertEquals(stmt.getUpdateCount(), 1);
            Assert.assertEquals(stmt.getLargeUpdateCount(), 1L);
            Assert.assertTrue(stmt.executeLargeUpdate("insert into test_execute_query values(1,'1')") >= 0L,
                    "Should return value greater than or equal to zero");
            Assert.assertNull(stmt.getResultSet(), "Should have no result set");
            Assert.assertEquals(stmt.getUpdateCount(), 1);
            Assert.assertEquals(stmt.getLargeUpdateCount(), 1L);

            stmt.addBatch("select 1");
            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeUpdate("insert into test_execute_query values(1,'1')"));
            Assert.assertThrows(SQLException.class,
                    () -> stmt.executeLargeUpdate("insert into test_execute_query values(1,'1')"));
            stmt.clearBatch();

            Assert.assertTrue(stmt.executeUpdate("insert into test_execute_query values(2,'2')") >= 0,
                    "Should return value greater than or equal to zero");
            Assert.assertNull(stmt.getResultSet(), "Should have no result set");
            Assert.assertEquals(stmt.getUpdateCount(), 1);
            Assert.assertEquals(stmt.getLargeUpdateCount(), 1L);
            Assert.assertTrue(stmt.executeLargeUpdate("insert into test_execute_query values(2,'2')") >= 0,
                    "Should return value greater than or equal to zero");
            Assert.assertNull(stmt.getResultSet(), "Should have no result set");
            Assert.assertEquals(stmt.getUpdateCount(), 1);
            Assert.assertEquals(stmt.getLargeUpdateCount(), 1L);
        }
    }

    @Test(groups = "integration")
    public void testFetchSize() throws SQLException {
        try (Connection conn = newConnection(new Properties()); Statement stmt = conn.createStatement()) {
            Assert.assertEquals(stmt.getFetchSize(), 0);

            stmt.setFetchSize(0);
            Assert.assertEquals(stmt.getFetchSize(), 0);
            stmt.setFetchSize(-1);
            Assert.assertEquals(stmt.getFetchSize(), 0);
            stmt.setFetchSize(Integer.MIN_VALUE);
            Assert.assertEquals(stmt.getFetchSize(), 0);

            stmt.setFetchSize(1);
            Assert.assertEquals(stmt.getFetchSize(), 1);
            stmt.setFetchSize(Integer.MAX_VALUE);
            Assert.assertEquals(stmt.getFetchSize(), Integer.MAX_VALUE);
            stmt.setFetchSize(0);
            Assert.assertEquals(stmt.getFetchSize(), 0);
        }
    }

    @Test(groups = "integration")
    public void testSimpleAggregateFunction() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                ClickHouseStatement stmt = conn.createStatement();) {
            stmt.execute("drop table if exists test_simple_agg_func; "
                    + "CREATE TABLE test_simple_agg_func (x SimpleAggregateFunction(max, UInt64)) ENGINE=AggregatingMergeTree ORDER BY tuple(); "
                    + "INSERT INTO test_simple_agg_func VALUES(1)");

            try (ResultSet rs = stmt.executeQuery("select * from test_simple_agg_func")) {
                Assert.assertTrue(rs.next(), "Should have one row");
                Assert.assertEquals(rs.getLong(1), 1L);
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
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
    public void testQuerySystemLog() throws SQLException {
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.RESULT_OVERFLOW_MODE.getKey(), "break");
        try (ClickHouseConnection conn = newConnection(props)) {
            ClickHouseStatement stmt = conn.createStatement();
            stmt.setMaxRows(10);
            stmt.setLargeMaxRows(11L);
            ResultSet rs = stmt.executeQuery("select * from numbers(100)");

            int rows = 0;
            try (ResultSet colRs = conn.getMetaData().getColumns(null, "system", "query_log", "")) {
                while (colRs.next()) {
                    continue;
                }
            }

            while (rs.next()) {
                rows++;
            }
            Assert.assertEquals(rows, 11);

            // batch query
            stmt.addBatch("drop table if exists non_existing_table1");
            stmt.addBatch("drop table if exists non_existing_table2");
            stmt.addBatch("drop table if exists non_existing_table3");
            int[] results = stmt.executeBatch();
            Assert.assertEquals(results, new int[] { 0, 0, 0 });
        }
    }

    @Test(groups = "integration")
    public void testQueryWithFormat() throws SQLException {
        try (Connection conn = newConnection(new Properties())) {
            Statement stmt = conn.createStatement();

            for (String[] pair : new String[][] { new String[] { "TSV", "1" },
                    new String[] { "JSONEachRow", "{\"1\":1}" } }) {
                try (ResultSet rs = stmt.executeQuery(String.format("select 1 format %s", pair[0]))) {
                    Assert.assertTrue(rs.next(), "Should have at least one row");
                    Assert.assertEquals(rs.getString(1), pair[1]);
                    Assert.assertFalse(rs.next(), "Should have only one row");
                }
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

    @Test(groups = "integration")
    public void testTimestampWithNanoSeconds() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists test_timetamp_with_nanos;"
                    + "create table test_timetamp_with_nanos(d DateTime64(9))engine=Memory");
            Instant instant = Instant.now();
            Timestamp now = new Timestamp(instant.toEpochMilli());
            now.setNanos(instant.getNano());
            try (PreparedStatement ps1 = conn.prepareStatement("insert into test_timetamp_with_nanos");
                    PreparedStatement ps2 = conn
                            .prepareStatement(
                                    "insert into test_timetamp_with_nanos values(toDateTime64(?, 9))")) {
                ps1.setTimestamp(1, now);
                ps1.executeUpdate();

                ps2.setTimestamp(1, now);
                ps2.executeUpdate();
            }
            ResultSet rs = stmt.executeQuery("select distinct * from test_timetamp_with_nanos");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1, Instant.class), instant);
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
            Assert.assertEquals(rs.getByte(1), (byte) 1);
            Assert.assertEquals(rs.getShort(1), (short) 1);
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(1), "a");
            Assert.assertEquals(rs.getString(1), "a");
            Assert.assertFalse(rs.next());
        }

        props.setProperty("typeMappings",
                "Enum8=java.lang.Byte,DateTime64=java.lang.String, String=com.clickhouse.data.ClickHouseDataType");
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
    public void testPrimitiveTypes() throws SQLException {
        String sql = "select toInt8(1), toUInt8(1), toInt16(1), toUInt16(1), toInt32(1), toUInt32(1), toInt64(1), toUInt64(1), "
                + "cast([1] as Array(Int8)), cast([1] as Array(UInt8)), cast([1] as Array(Int16)), cast([1] as Array(UInt16)), "
                + "cast([1] as Array(Int32)), cast([1] as Array(UInt32)), cast([1] as Array(Int64)), cast([1] as Array(UInt64))";

        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue(rs.next());
            int index = 1;
            Assert.assertEquals(rs.getObject(index++), (byte) 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedByte.ONE);
            Assert.assertEquals(rs.getObject(index++), (short) 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedShort.ONE);
            Assert.assertEquals(rs.getObject(index++), 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedInteger.ONE);
            Assert.assertEquals(rs.getObject(index++), 1L);
            Assert.assertEquals(rs.getObject(index++), UnsignedLong.ONE);
            Assert.assertEquals(rs.getObject(index++), new byte[] { (byte) 1 });
            Assert.assertEquals(rs.getObject(index++), new byte[] { (byte) 1 });
            Assert.assertEquals(rs.getObject(index++), new short[] { (short) 1 });
            Assert.assertEquals(rs.getObject(index++), new short[] { (short) 1 });
            Assert.assertEquals(rs.getObject(index++), new int[] { 1 });
            Assert.assertEquals(rs.getObject(index++), new int[] { 1 });
            Assert.assertEquals(rs.getObject(index++), new long[] { 1L });
            Assert.assertEquals(rs.getObject(index++), new long[] { 1L });
            Assert.assertFalse(rs.next());
        }

        props.clear();
        props.setProperty("use_objects_in_arrays", "true");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue(rs.next());
            int index = 1;
            Assert.assertEquals(rs.getObject(index++), (byte) 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedByte.ONE);
            Assert.assertEquals(rs.getObject(index++), (short) 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedShort.ONE);
            Assert.assertEquals(rs.getObject(index++), 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedInteger.ONE);
            Assert.assertEquals(rs.getObject(index++), 1L);
            Assert.assertEquals(rs.getObject(index++), UnsignedLong.ONE);
            Assert.assertEquals(rs.getObject(index++), new byte[] { (byte) 1 });
            Assert.assertEquals(rs.getObject(index++), new UnsignedByte[] { UnsignedByte.ONE });
            Assert.assertEquals(rs.getObject(index++), new short[] { (short) 1 });
            Assert.assertEquals(rs.getObject(index++), new UnsignedShort[] { UnsignedShort.ONE });
            Assert.assertEquals(rs.getObject(index++), new int[] { 1 });
            Assert.assertEquals(rs.getObject(index++), new UnsignedInteger[] { UnsignedInteger.ONE });
            Assert.assertEquals(rs.getObject(index++), new Long[] { 1L });
            Assert.assertEquals(rs.getObject(index++), new UnsignedLong[] { UnsignedLong.ONE });
            Assert.assertFalse(rs.next());
        }

        props.clear();
        props.setProperty("widen_unsigned_types", "false");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue(rs.next());
            int index = 1;
            Assert.assertEquals(rs.getObject(index++), (byte) 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedByte.ONE);
            Assert.assertEquals(rs.getObject(index++), (short) 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedShort.ONE);
            Assert.assertEquals(rs.getObject(index++), 1);
            Assert.assertEquals(rs.getObject(index++), UnsignedInteger.ONE);
            Assert.assertEquals(rs.getObject(index++), 1L);
            Assert.assertEquals(rs.getObject(index++), UnsignedLong.ONE);
            Assert.assertEquals(rs.getObject(index++), new byte[] { (byte) 1 });
            Assert.assertEquals(rs.getObject(index++), new byte[] { (byte) 1 });
            Assert.assertEquals(rs.getObject(index++), new short[] { (short) 1 });
            Assert.assertEquals(rs.getObject(index++), new short[] { (short) 1 });
            Assert.assertEquals(rs.getObject(index++), new int[] { 1 });
            Assert.assertEquals(rs.getObject(index++), new int[] { 1 });
            Assert.assertEquals(rs.getObject(index++), new long[] { 1L });
            Assert.assertEquals(rs.getObject(index++), new long[] { 1L });
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testNestedArrayInTuple() throws SQLException {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            // nested values on same row
            Assert.assertFalse(stmt.execute("drop table if exists test_nested_array_in_tuple; "
                    + "create table test_nested_array_in_tuple(id UInt64, v1 Tuple(Array(Int32)), v2 Tuple(Array(Int32)))engine=Memory; "
                    + "insert into test_nested_array_in_tuple values(1, ([1, 2]), ([2, 3]))"));
            try (ResultSet rs = stmt.executeQuery("select * from test_nested_array_in_tuple order by id")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(((List<?>) rs.getObject(2)).size(), 1);
                Assert.assertEquals(((List<?>) rs.getObject(2)).get(0), new int[] { 1, 2 });
                Assert.assertEquals(((List<?>) rs.getObject(3)).size(), 1);
                Assert.assertEquals(((List<?>) rs.getObject(3)).get(0), new int[] { 2, 3 });
                Assert.assertFalse(rs.next());
            }

            // nested values on same column
            Assert.assertFalse(stmt.execute("drop table if exists test_nested_array_in_tuple; "
                    + "create table test_nested_array_in_tuple(id UInt64, val Tuple(Array(Int32)))engine=Memory; "
                    + "insert into test_nested_array_in_tuple values(1, ([1, 2])), (2, ([2, 3]))"));
            try (ResultSet rs = stmt.executeQuery("select * from test_nested_array_in_tuple order by id")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(((List<?>) rs.getObject(2)).size(), 1);
                Assert.assertEquals(((List<?>) rs.getObject(2)).get(0), new int[] { 1, 2 });
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 2);
                Assert.assertEquals(((List<?>) rs.getObject(2)).size(), 1);
                Assert.assertEquals(((List<?>) rs.getObject(2)).get(0), new int[] { 2, 3 });
                Assert.assertFalse(rs.next());
            }

            // deeper nested level and more elements
            Assert.assertFalse(stmt.execute("drop table if exists test_nested_array_in_tuple; "
                    + "create table test_nested_array_in_tuple(id UInt64, val Array(Tuple(UInt16,Array(UInt32))))engine=Memory; "
                    + "insert into test_nested_array_in_tuple values(1, [(0, [1, 2]), (1, [2, 3])]), (2, [(2, [4, 5]), (3, [6, 7])])"));
            try (ResultSet rs = stmt.executeQuery("select * from test_nested_array_in_tuple order by id")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(((Object[]) rs.getObject(2)).length, 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).get(0), UnsignedShort.ZERO);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).get(1), new int[] { 1, 2 });
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).get(0), UnsignedShort.ONE);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).get(1), new int[] { 2, 3 });
                Assert.assertTrue(rs.next());
                Assert.assertEquals(((Object[]) rs.getObject(2)).length, 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).get(0),
                        UnsignedShort.valueOf((short) 2));
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).get(1), new int[] { 4, 5 });
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).get(0),
                        UnsignedShort.valueOf((short) 3));
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).get(1), new int[] { 6, 7 });
                Assert.assertFalse(rs.next());
            }

            Assert.assertFalse(stmt.execute("drop table if exists test_nested_array_in_tuple; "
                    + "create table test_nested_array_in_tuple(id UInt64, val Array(Tuple(UInt16,Array(Decimal(10,0)))))engine=Memory; "
                    + "insert into test_nested_array_in_tuple values(1, [(0, [1, 2]), (1, [2, 3])]), (2, [(2, [4, 5]), (3, [6, 7])])"));
            try (ResultSet rs = stmt.executeQuery("select * from test_nested_array_in_tuple order by id")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(((Object[]) rs.getObject(2)).length, 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).get(0), UnsignedShort.ZERO);
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[0]).get(1))[0],
                        BigDecimal.valueOf(1));
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[0]).get(1))[1],
                        BigDecimal.valueOf(2));
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).get(0), UnsignedShort.ONE);
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[1]).get(1))[0],
                        BigDecimal.valueOf(2));
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[1]).get(1))[1],
                        BigDecimal.valueOf(3));
                Assert.assertTrue(rs.next());
                Assert.assertEquals(((Object[]) rs.getObject(2)).length, 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[0]).get(0),
                        UnsignedShort.valueOf((short) 2));
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[0]).get(1))[0],
                        BigDecimal.valueOf(4));
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[0]).get(1))[1],
                        BigDecimal.valueOf(5));
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).size(), 2);
                Assert.assertEquals(((List<?>) ((Object[]) rs.getObject(2))[1]).get(0),
                        UnsignedShort.valueOf((short) 3));
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[1]).get(1))[0],
                        BigDecimal.valueOf(6));
                Assert.assertEquals(((BigDecimal[]) ((List<?>) ((Object[]) rs.getObject(2))[1]).get(1))[1],
                        BigDecimal.valueOf(7));
                Assert.assertFalse(rs.next());
            }
        }
    }
    
    @Test(groups = "integration")
    public void testNestedArrays() throws SQLException {
        Properties props = new Properties();
        Object[][] arr1 = null;
        Object[][] arr2 = null;
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery(
                                "select * from (select 1 id, [['1','2'],['3', '4']] v union all select 2 id, [['5','6'],['7','8']] v) order by id")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Assert.assertEquals(rs.getObject(2), arr1 = (Object[][]) rs.getArray(2).getArray());
            Assert.assertEquals(((Object[][]) rs.getObject(2)).length, 2);
            Assert.assertEquals(((Object[][]) rs.getObject(2))[0], new Object[] { "1", "2" });
            Assert.assertEquals(((Object[][]) rs.getObject(2))[1], new Object[] { "3", "4" });
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 2);
            Assert.assertEquals(rs.getObject(2), arr2 = (Object[][]) rs.getArray(2).getArray());
            Assert.assertEquals(((Object[][]) rs.getObject(2)).length, 2);
            Assert.assertEquals(((Object[][]) rs.getObject(2))[0], new Object[] { "5", "6" });
            Assert.assertEquals(((Object[][]) rs.getObject(2))[1], new Object[] { "7", "8" });
            Assert.assertFalse(rs.next());
        }

        Assert.assertTrue(arr1 != arr2);
        Assert.assertNotEquals(arr1[0], arr2[0]);
        Assert.assertNotEquals(arr1[1], arr2[1]);
    }

    @Test(groups = "integration")
    public void testNestedDataTypes() throws SQLException {
        String sql = "select (1,2) as t, [3,4] as a";
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), Arrays.asList(UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2)));
            Assert.assertEquals(rs.getObject(2), new byte[] { (byte) 3, (byte) 4 });
            Assert.assertFalse(rs.next());
        }

        props.setProperty("use_objects_in_arrays", "true");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), Arrays.asList(UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2)));
            Assert.assertEquals(rs.getObject(2),
                    new UnsignedByte[] { UnsignedByte.valueOf((byte) 3), UnsignedByte.valueOf((byte) 4) });
            Assert.assertFalse(rs.next());
        }

        props.clear();
        props.setProperty("widen_unsigned_types", "true");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), Arrays.asList((short) 1, (short) 2));
            Assert.assertEquals(rs.getObject(2), new short[] { (short) 3, (short) 4 });
            Assert.assertFalse(rs.next());
        }

        props.clear();
        props.setProperty("wrapperObject", "true");
        try (ClickHouseConnection conn = newConnection(props);
                ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(((ClickHouseStruct) rs.getObject(1)).getAttributes(),
                    new Object[] { UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2) });
            Assert.assertEquals(((ClickHouseArray) rs.getObject(2)).getArray(), rs.getArray(2).getArray());
            Assert.assertFalse(rs.next());
        }
    }

    @Test(dataProvider = "timeZoneTestOptions", groups = "integration")
    public void testTimeZone(boolean useBinary) throws SQLException {
        if (isCloud()) return; //TODO: testTimeZone - Revisit, see: https://github.com/ClickHouse/clickhouse-java/issues/1747
        String dateType = "DateTime32";
        String dateValue = "2020-02-11 00:23:33";
        ClickHouseDateTimeValue v = ClickHouseDateTimeValue.of(dateValue, 0, ClickHouseValues.UTC_TIMEZONE);

        Properties props = new Properties();
        String[] timeZones = new String[] { "Asia/Chongqing", "America/Los_Angeles", "Europe/Moscow", "Etc/UTC",
                "Europe/Berlin" };
        StringBuilder columns = new StringBuilder().append("d0 ").append(dateType);
        StringBuilder constants = new StringBuilder().append(ClickHouseValues.convertToQuotedString(dateValue));
        StringBuilder currents = new StringBuilder().append("now()");
        StringBuilder parameters = new StringBuilder().append(useBinary ? "?,?" : "trim(?),?");
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
                    + ") engine=Memory;" + "insert into test_tz Values ('0 - Constant'," + constants.toString() + ");"
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

    @Test(groups = "integration")
    public void testMultiThreadedExecution() throws Exception {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
             ClickHouseStatement stmt = conn.createStatement()) {


            ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);

            final AtomicReference<Exception> failedException = new AtomicReference<>(null);
            for (int i = 0; i < 3; i++) {
                executor.scheduleWithFixedDelay(() -> {
                    try {
                        stmt.execute("select 1");
                    } catch (Exception e) {
                        failedException.set(e);
                    }
                }, 100, 100, TimeUnit.MILLISECONDS);
            }

            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                Assert.fail("Test interrupted", e);
            }

            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            Assert.assertNull(failedException.get(), "Failed because of exception: " + failedException.get());
         }
    }

    @Test(groups = "integration")
    public void testSessionTimezoneSetting() {
        Properties props = new Properties();
        try (ClickHouseConnection conn = newConnection(props);
             ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT now() SETTINGS session_timezone = 'America/Los_Angeles'");
            rs.next();
            OffsetDateTime srvNow = rs.getObject(1, OffsetDateTime.class);
            OffsetDateTime localNow = OffsetDateTime.now(ZoneId.of("America/Los_Angeles"));
            Assert.assertTrue(Duration.between(srvNow, localNow).abs().getSeconds() < 60,
                    "server time (" + srvNow +") differs from local time (" + localNow + ")");
        } catch (Exception e) {
            Assert.fail("Failed to create connection", e);
        }
    }


    @Test(groups = "integration")
    public void testUseOffsetDateTime() {
        try (ClickHouseConnection conn = newConnection();
             ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select toDateTime('2024-01-01 10:00:00', 'America/Los_Angeles'), toDateTime('2024-05-01 10:00:00', " +
                    " 'America/Los_Angeles'), now() SETTINGS session_timezone = 'America/Los_Angeles'");
            rs.next();
            OffsetDateTime dstStart = (OffsetDateTime) rs.getObject(1);
            OffsetDateTime dstEnd = (OffsetDateTime) rs.getObject(2);
            OffsetDateTime now = rs.getObject(3, OffsetDateTime.class);
            System.out.println("dstStart: " + dstStart + ", dstEnd: " + dstEnd + ", now: " + now);
            Assert.assertEquals(dstStart.getOffset(), ZoneOffset.ofHours(-8));
            Assert.assertEquals(dstEnd.getOffset(), ZoneOffset.ofHours(-7));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to create connection", e);
        }
    }


    @Test(groups = "integration")
    public void testDescMetadata() {
        try (ClickHouseConnection conn = newConnection();
            ClickHouseStatement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("DESC (select timezone(), number FROM system.numbers)");
            rs.next();
            ResultSetMetaData metaData = rs.getMetaData();
            Assert.assertEquals(metaData.getColumnCount(), 7);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Failed to create connection", e);
        }
    }

    @Test(groups = "integration")
    public void testMaxResultsRows() throws SQLException {
        Properties props = new Properties();
        int maxRows = 3;
        props.setProperty(ClickHouseClientOption.MAX_RESULT_ROWS.getKey(), String.valueOf(maxRows));
        props.setProperty(ClickHouseClientOption.RESULT_OVERFLOW_MODE.getKey(), "break");
        try (ClickHouseConnection conn = newConnection(props);
             ClickHouseStatement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT number FROM system.numbers");
            for (int i = 0; i < maxRows; i++) {
                Assert.assertTrue(rs.next(), "Should have more rows, but have only " + i);
            }
        }

        props.setProperty(ClickHouseClientOption.MAX_RESULT_ROWS.getKey(), "1");
        props.remove(ClickHouseClientOption.RESULT_OVERFLOW_MODE.getKey());
        try (ClickHouseConnection conn = newConnection(props);
             ClickHouseStatement s = conn.createStatement()) {
            s.executeQuery("SELECT number FROM system.numbers");
            Assert.fail("Should throw exception");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().startsWith("Code: 396. DB::Exception: Limit for result exceeded, max rows"),
                    "Unexpected exception: " + e.getMessage());
        }
    }

    @Test(groups = "integration", enabled = false)
    public void testVariantDataType() throws SQLException {
        String table = "test_variant_type_01";
        Properties props = new Properties();
        props.setProperty("custom_settings", "allow_experimental_variant_type=1");
        props.setProperty(ClickHouseClientOption.COMPRESS.getKey(), "false");
        try (ClickHouseConnection conn = newConnection(props);
             ClickHouseStatement s = conn.createStatement()) {

            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE " + table +" ( id Variant(UInt32, String, UUID), name String) Engine = MergeTree ORDER BY ()");

            s.execute("insert into " + table + " values ( 1, 'just number' )");
            s.execute("insert into " + table + " values  ( 'i-am-id-01', 'ID as string' ) ");
            s.execute("insert into " + table + " values  ( generateUUIDv4(), 'ID as UUID' ) ");

            try (ResultSet rs = s.executeQuery("SELECT * FROM " + table)) {
                while (rs.next()) {
                    Object variantValue = rs.getObject(1);
                    Object name = rs.getString(2);
                    System.out.println("-> " + name + " : " + variantValue);
                }
            }
        }
    }
}
