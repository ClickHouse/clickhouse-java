package com.clickhouse.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.BiFunction;

import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseValues;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseOffsetDateTimeValue;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseResultSetTest extends JdbcIntegrationTest {
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

            Assert.assertEquals(rs.getObject(1), new short[] { 1, 2, 3 });
            Assert.assertEquals(rs.getArray(1).getArray(), new short[] { 1, 2, 3 });
            Assert.assertTrue(rs.getArray(1).getArray() == rs.getObject(1));

            Assert.assertEquals(rs.getObject(2), new String[] { "a", "b", "c" });
            Assert.assertEquals(rs.getArray(2).getArray(), new String[] { "a", "b", "c" });
            Assert.assertTrue(rs.getArray(2).getArray() == rs.getObject(2));

            Assert.assertEquals(rs.getObject(3), new List[] { Arrays.asList((short) 1, "a"),
                    Arrays.asList((short) 2, "b"), Arrays.asList((short) 3, "c") });
            Assert.assertEquals(rs.getArray(3).getArray(), new List[] { Arrays.asList((short) 1, "a"),
                    Arrays.asList((short) 2, "b"), Arrays.asList((short) 3, "c") });
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
            ResultSet rs = stmt
                    .executeQuery(
                            "select (toInt16(1), 'a', toFloat32(1.2), cast([1,2] as Array(Nullable(UInt8))), map(toUInt32(1),'a')) v");
            Assert.assertTrue(rs.next());
            List<?> v = rs.getObject(1, List.class);
            Assert.assertEquals(v.size(), 5);
            Assert.assertEquals(v.get(0), Short.valueOf((short) 1));
            Assert.assertEquals(v.get(1), "a");
            Assert.assertEquals(v.get(2), Float.valueOf(1.2F));
            Assert.assertEquals(v.get(3), new Short[] { 1, 2 });
            Assert.assertEquals(v.get(4), Collections.singletonMap(1L, "a"));
            Assert.assertFalse(rs.next());
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
    public void testNullValue(String columnType, String defaultValue) throws Exception {
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
                Assert.assertFalse(rs.wasNull(), "Should not be null");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            s.setNullAsDefault(1);
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id", tableName))) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), null);
                Assert.assertTrue(rs.wasNull(), "Should be null");
                Assert.assertFalse(rs.next(), "Should have only one row");
            }

            s.setNullAsDefault(0);
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by id", tableName))) {
                Assert.assertTrue(rs.next(), "Should have at least one row");
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), null);
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
}
