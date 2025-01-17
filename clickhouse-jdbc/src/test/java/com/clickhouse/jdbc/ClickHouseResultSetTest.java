package com.clickhouse.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.BiFunction;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseSimpleResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseValues;
import com.clickhouse.data.value.ClickHouseDateTimeValue;
import com.clickhouse.data.value.ClickHouseOffsetDateTimeValue;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseResultSetTest extends JdbcIntegrationTest {
    @BeforeMethod(groups = "integration")
    public void setV1() {
        System.setProperty("clickhouse.jdbc.v1","true");
    }
    @DataProvider(name = "nullableTypes")
    private Object[][] getNullableTypes() {
        return new Object[][] {
                new Object[] { ClickHouseDataType.Int32, Integer.valueOf(12345),
                        new BiFunction<ResultSet, Integer, Object>() {
                            @Override
                            public Object apply(ResultSet rs, Integer i) {
                                try {
                                    Object obj = rs.getInt(i);
                                    if (obj != null) {
                                        obj = rs.getFloat(i);
                                    }
                                    if (obj != null) {
                                        obj = rs.getBigDecimal(i);
                                    }
                                    return obj;
                                } catch (SQLException e) {
                                    throw new IllegalArgumentException(e);
                                }
                            }
                        } },
                new Object[] { ClickHouseDataType.Date, LocalDate.of(2022, 1, 7),
                        new BiFunction<ResultSet, Integer, Object>() {
                            @Override
                            public Object apply(ResultSet rs, Integer i) {
                                try {
                                    Object obj = rs.getDate(i);
                                    if (obj != null) {
                                        obj = rs.getTime(i);
                                    }
                                    if (obj != null) {
                                        obj = rs.getTimestamp(i);
                                    }
                                    return obj;
                                } catch (SQLException e) {
                                    throw new IllegalArgumentException(e);
                                }
                            }
                        } },
                new Object[] { ClickHouseDataType.DateTime, LocalDateTime.of(2022, 1, 7, 19, 11, 55),
                        new BiFunction<ResultSet, Integer, Object>() {
                            @Override
                            public Object apply(ResultSet rs, Integer i) {
                                try {
                                    Object obj = rs.getDate(i);
                                    if (obj != null) {
                                        obj = rs.getTime(i);
                                    }
                                    if (obj != null) {
                                        obj = rs.getTimestamp(i);
                                    }
                                    return obj;
                                } catch (SQLException e) {
                                    throw new IllegalArgumentException(e);
                                }
                            }
                        } }
        };
    }

    @DataProvider(name = "nullableColumns")
    private Object[][] getNullableColumns() {
        return new Object[][] {
                new Object[] { "Bool", "false", Boolean.class },
                new Object[] { "Date", "1970-01-01", LocalDate.class },
                new Object[] { "Date32", "1970-01-01", LocalDate.class },
                new Object[] { "DateTime32('UTC')", "1970-01-01 00:00:00", LocalDateTime.class },
                new Object[] { "DateTime64(3, 'UTC')", "1970-01-01 00:00:00", OffsetDateTime.class },
                new Object[] { "Decimal(10,4)", "0", BigDecimal.class },
                new Object[] { "Enum8('x'=0,'y'=1)", "x", Integer.class },
                new Object[] { "Enum16('xx'=1,'yy'=0)", "yy", String.class },
                new Object[] { "Float32", "0.0", Float.class },
                new Object[] { "Float64", "0.0", Double.class },
                new Object[] { "Int8", "0", Byte.class },
                new Object[] { "UInt8", "0", Short.class },
                new Object[] { "Int16", "0", Short.class },
                new Object[] { "UInt16", "0", Integer.class },
                new Object[] { "Int32", "0", Integer.class },
                new Object[] { "UInt32", "0", Long.class },
                new Object[] { "Int64", "0", Long.class },
                new Object[] { "UInt64", "0", BigInteger.class },
                new Object[] { "Int128", "0", BigInteger.class },
                new Object[] { "UInt128", "0", BigInteger.class },
                new Object[] { "Int256", "0", BigInteger.class },
                new Object[] { "UInt256", "0", BigInteger.class },
        };
    }

    @Test(groups = "integration")
    public void testFloatToBigDecimal() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select toFloat32(1.35) fp, toFloat32(-1.35) fn, "
                        + "toFloat64(1.35) dp, toFloat64(-1.35) dn, "
                        + "toDecimal64(1.35, 1) p1, toDecimal64(1.35, 2) p2, "
                        + "toDecimal64(-1.35, 1) n1, toDecimal64(-1.35, 2) n2")) {
            while (rs.next()) {
                ClickHouseRecord r = rs.unwrap(ClickHouseRecord.class);
                Assert.assertEquals(r.getValue("fp").asBigDecimal(), r.getValue("p2").asObject());
                Assert.assertEquals(r.getValue("fn").asBigDecimal(), r.getValue("n2").asObject());
                Assert.assertEquals(r.getValue("dp").asBigDecimal(), r.getValue("p2").asObject());
                Assert.assertEquals(r.getValue("dn").asBigDecimal(), r.getValue("n2").asObject());
                for (int i = 1; i <= 2; i++) {
                    Assert.assertEquals(r.getValue("fp").asBigDecimal(i), r.getValue("p" + i).asObject());
                    Assert.assertEquals(r.getValue("fn").asBigDecimal(i), r.getValue("n" + i).asObject());
                    Assert.assertEquals(r.getValue("dp").asBigDecimal(i), r.getValue("p" + i).asObject());
                    Assert.assertEquals(r.getValue("dn").asBigDecimal(i), r.getValue("n" + i).asObject());

                    Assert.assertEquals(rs.getBigDecimal("fp", i), rs.getBigDecimal("p" + i));
                    Assert.assertEquals(rs.getBigDecimal("fn", i), rs.getBigDecimal("n" + i));
                    Assert.assertEquals(rs.getBigDecimal("dp", i), rs.getBigDecimal("p" + i));
                    Assert.assertEquals(rs.getBigDecimal("dn", i), rs.getBigDecimal("n" + i));
                }
            }
        }
    }

    @Test(groups = "integration")
    public void testBigDecimal() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select toDecimal64(number / 10, 1) from numbers(10)")) {
            BigDecimal v = BigDecimal.valueOf(0L).setScale(1);
            while (rs.next()) {
                Assert.assertEquals(rs.getBigDecimal(1), v);
                Assert.assertEquals(rs.getObject(1), v);
                v = v.add(new BigDecimal("0.1"));
            }
        }
    }

    @Test(groups = "integration")
    public void testArray() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "select [1,2,3] v1, ['a','b', 'c'] v2, arrayZip(v1, v2) v3, cast(['2021-11-01 01:02:03', '2021-11-02 02:03:04'] as Array(DateTime32)) v4");
            Assert.assertTrue(rs.next());

            Assert.assertEquals(rs.getObject(1), new byte[] { 1, 2, 3 });
            Assert.assertEquals(rs.getArray(1).getArray(), new byte[] { 1, 2, 3 });
            Assert.assertTrue(rs.getArray(1).getArray() == rs.getObject(1));

            Assert.assertEquals(rs.getObject(2), new String[] { "a", "b", "c" });
            Assert.assertEquals(rs.getArray(2).getArray(), new String[] { "a", "b", "c" });
            Assert.assertTrue(rs.getArray(2).getArray() == rs.getObject(2));

            Assert.assertEquals(rs.getObject(3), new List[] { Arrays.asList(UnsignedByte.ONE, "a"),
                    Arrays.asList(UnsignedByte.valueOf((byte) 2), "b"),
                    Arrays.asList(UnsignedByte.valueOf((byte) 3), "c") });
            Assert.assertEquals(rs.getArray(3).getArray(), new List[] { Arrays.asList(UnsignedByte.ONE, "a"),
                    Arrays.asList(UnsignedByte.valueOf((byte) 2), "b"),
                    Arrays.asList(UnsignedByte.valueOf((byte) 3), "c") });
            Assert.assertTrue(rs.getArray(3).getArray() == rs.getObject(3));

            Assert.assertEquals(rs.getObject(4), new LocalDateTime[] { LocalDateTime.of(2021, 11, 1, 1, 2, 3),
                    LocalDateTime.of(2021, 11, 2, 2, 3, 4) });
            Assert.assertEquals(rs.getArray(4).getArray(), new LocalDateTime[] { LocalDateTime.of(2021, 11, 1, 1, 2, 3),
                    LocalDateTime.of(2021, 11, 2, 2, 3, 4) });
            Assert.assertTrue(rs.getArray(4).getArray() == rs.getObject(4));

            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testHugeNumber() throws SQLException {
        String number = "15369343623947579499";
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt
                        .executeQuery(String.format("SELECT toUInt64(%1$s) a, toNullable(%1$s) b", number))) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), number);
            Assert.assertEquals(rs.getString(2), number);
            Assert.assertEquals(rs.getBigDecimal(1), new BigDecimal(number));
            Assert.assertEquals(rs.getBigDecimal(2), new BigDecimal(number));
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testIpAddress() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt
                    .executeQuery("select toIPv4('116.253.40.133'), toIPv6('2001:44c8:129:2632:33:0:252:2')");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), "116.253.40.133");
            Assert.assertEquals(rs.getObject(1).toString(), "/116.253.40.133");
            Assert.assertEquals(rs.getString(2), "2001:44c8:129:2632:33:0:252:2");
            Assert.assertEquals(rs.getObject(2).toString(), "/2001:44c8:129:2632:33:0:252:2");
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testMap() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            if (!conn.getServerVersion().check("[21.8,)")) {
                throw new SkipException("Skip test when ClickHouse version is older than 21.8");
            }

            stmt.execute("drop table if exists test_map_of_array; "
                    + "create table test_map_of_array(id Int8, m0 Map(String, Array(Nullable(DateTime64(3)))), m1 Map(String, Array(Nullable(DateTime64(3, 'Asia/Shanghai'))))) ENGINE = Memory; "
                    + "insert into test_map_of_array values(1, { 'a' : [], 'b' : [ '2022-03-30 00:00:00.123', null ] }, { 'a' : [], 'b' : [ '2022-03-30 00:00:00.123', null ] })");
            ResultSet rs = stmt
                    .executeQuery(
                            "select * from test_map_of_array order by id");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Map<?, ?> v = rs.getObject(2, Map.class);
            Assert.assertEquals(v.size(), 2);
            Assert.assertEquals(v.get("a"), new LocalDateTime[0]);
            Assert.assertEquals(v.get("b"),
                    new LocalDateTime[] {
                            ClickHouseDateTimeValue.ofNull(3, TimeZone.getTimeZone("Asia/Shanghai"))
                                    .update("2022-03-30 00:00:00.123").getValue(),
                            null });
            v = rs.getObject(3, Map.class);
            Assert.assertEquals(v.size(), 2);
            Assert.assertEquals(v.get("a"), new OffsetDateTime[0]);
            Assert.assertEquals(v.get("b"),
                    new OffsetDateTime[] {
                            ClickHouseOffsetDateTimeValue.ofNull(3, TimeZone.getTimeZone("Asia/Shanghai"))
                                    .update("2022-03-30 00:00:00.123").getValue(),
                            null });
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testTuple() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "select (toInt16(1), 'a', toFloat32(1.2), cast([1,2] as Array(Nullable(UInt8))), map(toUInt32(1),'a')) v");
            Assert.assertTrue(rs.next());
            List<?> v = rs.getObject(1, List.class);
            Assert.assertEquals(v.size(), 5);
            Assert.assertEquals(v.get(0), Short.valueOf((short) 1));
            Assert.assertEquals(v.get(1), "a");
            Assert.assertEquals(v.get(2), Float.valueOf(1.2F));
            Assert.assertEquals(v.get(3), new UnsignedByte[] { UnsignedByte.ONE, UnsignedByte.valueOf((byte) 2) });
            Assert.assertEquals(v.get(4), Collections.singletonMap(UnsignedInteger.ONE, "a"));
            Assert.assertFalse(rs.next());

            rs = stmt.executeQuery(
                    "select cast(tuple(1, [2,3], ('4', [5,6]), map('seven', 8)) as Tuple(Int16, Array(Nullable(Int16)), Tuple(String, Array(Int32)), Map(String, Int32))) v");
            Assert.assertTrue(rs.next());
            v = rs.getObject(1, List.class);
            Assert.assertEquals(v.size(), 4);
            Assert.assertEquals(v.get(0), Short.valueOf((short) 1));
            Assert.assertEquals(v.get(1), new Short[] { 2, 3 });
            Assert.assertEquals(((List<?>) v.get(2)).get(0), "4");
            Assert.assertEquals(((List<?>) v.get(2)).get(1), new int[] { 5, 6 });
            Assert.assertEquals(v.get(3), Collections.singletonMap("seven", 8));
            Assert.assertFalse(rs.next());
        }
    }

    @Test(groups = "integration")
    public void testNested() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {

            stmt.execute("set flatten_nested=0; "
                    + "drop table if exists test_simple_aggregate_nested; "
                    + "create table test_simple_aggregate_nested(id Int8, n0 SimpleAggregateFunction(anyLast, Nested(a String,b String))) ENGINE = AggregatingMergeTree() ORDER BY (id); "
                    + "insert into test_simple_aggregate_nested values(1, [tuple('foo1', 'bar1'), tuple('foo11', 'bar11')]), (2, [tuple('foo2', 'bar2'), tuple('foo22', 'bar22')])");
            ResultSet rs = stmt
                    .executeQuery(
                            "select * from test_simple_aggregate_nested");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 1);
            Map<?, ?> v = rs.getObject(2, Map.class);
            Assert.assertEquals(v.size(), 2);
            Assert.assertEquals(v.get("a"), new String[]{"foo1", "foo11"});
        }
    }

    @Test(dataProvider = "nullableTypes", groups = "integration")
    public void testNullableValues(ClickHouseDataType type, Object value, BiFunction<ResultSet, Integer, Object> func)
            throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            String table = "test_nullable_" + type.name().toLowerCase();
            String ddl = "drop table if exists " + table + "; create table " + table + "(v1 " + type.name()
                    + ", v2 Nullable(" + type.name() + "))engine=Memory;";
            String insert = "insert into " + table + " values(" + ClickHouseValues.convertToSqlExpression(value)
                    + ", null);";
            String query = "select * from " + table;

            ResultSet rs = stmt.executeQuery(ddl + insert + query);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getObject(1), value);
            Assert.assertNotNull(rs.getString(1));
            Assert.assertNotNull(func.apply(rs, 1));
            Assert.assertNull(rs.getObject(2));
            Assert.assertNull(rs.getString(2));
            Assert.assertNull(func.apply(rs, 2));
            Assert.assertFalse(rs.next());
        }
    }

    @Test(dataProvider = "nullableColumns", groups = "integration")
    public void testNullValue(String columnType, String defaultValue, Class<?> clazz) throws SQLException {
        Properties props = new Properties();
        props.setProperty(JdbcConfig.PROP_NULL_AS_DEFAULT, "2");
        String tableName = "test_query_null_value_" + columnType.split("\\(")[0].trim().toLowerCase();
        try (ClickHouseConnection conn = newConnection(props); ClickHouseStatement s = conn.createStatement()) {
            if (!conn.getServerVersion().check("[22.3,)")) {
                throw new SkipException("Skip test when ClickHouse is older than 22.3");
            }
            s.execute(String.format("drop table if exists %s; ", tableName)
                    + String.format("create table %s(id Int8, v Nullable(%s))engine=Memory; ", tableName, columnType)
                    + String.format("insert into %s values(1, null)", tableName));

            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id", tableName))) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), defaultValue);
                Assert.assertNotNull(rs.getObject(2));
                Assert.assertNotNull(rs.getObject(2, clazz));
                Assert.assertFalse(rs.wasNull(), "Should not be null");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            s.setNullAsDefault(1);
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id", tableName))) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), null);
                Assert.assertEquals(rs.getObject(2, clazz), null);
                Assert.assertTrue(rs.wasNull(), "Should be null");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            s.setNullAsDefault(0);
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id", tableName))) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), null);
                Assert.assertEquals(rs.getObject(2, clazz), null);
                Assert.assertTrue(rs.wasNull(), "Should be null");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }
        } catch (SQLException e) {
            // 'Unknown data type family', 'Missing columns' or 'Cannot create table column'
            if (e.getErrorCode() == 50 || e.getErrorCode() == 47 || e.getErrorCode() == 44) {
                return;
            }
            throw e;
        }
    }

    @Test(groups = "unit")
    public void testFetchSizeOfDetachedResultSet() throws SQLException {
        try (ClickHouseResultSet rs = new ClickHouseResultSet("", "",
                ClickHouseSimpleResponse.of(new ClickHouseConfig(), ClickHouseColumn.parse("s String"),
                        new Object[][] { new Object[] { "a" } }))) {
            Assert.assertEquals(rs.getFetchSize(), 0);
            rs.setFetchSize(2);
            Assert.assertEquals(rs.getFetchSize(), 0);
            rs.setFetchSize(-1);
            Assert.assertEquals(rs.getFetchSize(), 0);
        }
    }

    @Test(groups = "integration")
    public void testFetchSize() throws SQLException {
        try (ClickHouseConnection conn = newConnection(new Properties()); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.assertEquals(rs.getFetchSize(), 0);
                rs.setFetchSize(2);
                Assert.assertEquals(rs.getFetchSize(), 0);
                rs.setFetchSize(-1);
                Assert.assertEquals(rs.getFetchSize(), 0);
            }

            stmt.setFetchSize(1);
            try (ResultSet rs = stmt.executeQuery("select 1")) {
                Assert.assertEquals(rs.getFetchSize(), 1);
                rs.setFetchSize(2);
                Assert.assertEquals(rs.getFetchSize(), 1);
                rs.setFetchSize(-1);
                Assert.assertEquals(rs.getFetchSize(), 1);
            }
        }
    }


    @Test(groups = "integration")
    public void testDateTimeWithoutTimezone() throws SQLException {
        final String sql =  "select now(), toDateTime(now(), 'America/New_York') as tzTime SETTINGS session_timezone = 'America/New_York'";
        // Default behavior
        try (ClickHouseConnection conn = newConnection(new Properties());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            Assert.assertTrue(rs.next());
            OffsetDateTime serverNowOffseted = rs.getObject(1, OffsetDateTime.class);
            LocalDateTime serverNow = (LocalDateTime) rs.getObject(1);
            OffsetDateTime tzTime = (OffsetDateTime) rs.getObject(2);
            ZonedDateTime serverNowZoned = rs.getObject(1, ZonedDateTime.class);
            Assert.assertTrue(serverNow.isEqual(tzTime.toLocalDateTime()));
            Assert.assertTrue(serverNow.isEqual(serverNowOffseted.toLocalDateTime()));
            Assert.assertEquals(tzTime.getOffset(), TimeZone.getTimeZone("America/New_York").toZoneId().getRules().getOffset(tzTime.toInstant()));
            Assert.assertEquals(serverNowZoned.getZone(), TimeZone.getTimeZone("America/New_York").toZoneId());
            Assert.assertEquals(serverNowZoned.toLocalDateTime(), serverNow);

            Time serverNowTime = rs.getTime(1);
            Time tzTimeTime = rs.getTime(2);
            Timestamp serverNowTimestamp = rs.getTimestamp(1);
            Timestamp tzTimeTimestamp = rs.getTimestamp(2);
            Assert.assertEquals(serverNowTime, tzTimeTime);
            Assert.assertEquals(serverNowTimestamp, tzTimeTimestamp);
        }
    }
}
