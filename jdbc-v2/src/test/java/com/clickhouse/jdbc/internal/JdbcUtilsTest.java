package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class JdbcUtilsTest {


    @Test(groups = {"unit"})
    public void testConvertPrimitiveTypes() throws SQLException {
        assertEquals(JdbcUtils.convert(1, int.class), Integer.valueOf(1));
        assertEquals(JdbcUtils.convert(1L, long.class), Long.valueOf(1));
        assertEquals(JdbcUtils.convert("1", String.class), "1");
        assertEquals(JdbcUtils.convert(1.0f, float.class), Float.valueOf(1));
        assertEquals(JdbcUtils.convert(1.0, double.class), Double.valueOf(1));
        assertEquals(JdbcUtils.convert(true, boolean.class), Boolean.TRUE);
        assertEquals(JdbcUtils.convert((short) 1, short.class), Short.valueOf("1"));
        assertEquals(JdbcUtils.convert((byte) 1, byte.class), Byte.valueOf("1"));
        assertEquals(JdbcUtils.convert(1.0d, BigDecimal.class), new BigDecimal("1.0"));
    }


    @Test(groups = {"unit"})
    public void testConvertToArray() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("arr", "Array(Int32)");
        BinaryStreamReader.ArrayValue arrayValue = new BinaryStreamReader.ArrayValue(int.class, 2);
        arrayValue.set(0, 1);
        arrayValue.set(1, 2);
        java.sql.Array array = (java.sql.Array) JdbcUtils.convert(arrayValue, java.sql.Array.class, column);
        Object arr = array.getArray();
        assertEquals(array.getBaseTypeName(), "Int32");
        assertEquals(arr.getClass().getComponentType(), Integer.class);
        Integer[] arrs = (Integer[]) arr;
        assertEquals(arrs[0], 1);
        assertEquals(arrs[1], 2);
    }


    @Test(groups = {"unit"})
    public void testConvertArray() throws Exception {
        // primitive classes are unwrapped
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Boolean.class), new Boolean[] { false, true });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Byte.class), new Byte[] { 0, 1 });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Short.class), new Short[] { 0, 1 });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Integer.class), new Integer[] { 0, 1 });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Long.class), new Long[] { 0L, 1L });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Float.class), new Float[] { 0.0f, 1.0f });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, Double.class), new Double[] { 0.0, 1.0 });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, String.class), new String[] { "0", "1" });
        assertEquals(JdbcUtils.convertArray(new Object[] { 0, 1 }, BigDecimal.class), new BigDecimal[] { BigDecimal.valueOf(0), BigDecimal.valueOf(1) });
        assertNull(JdbcUtils.convertArray(null, Integer.class));
    }


    @Test(groups = {"unit"})
    public void testConvertList() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("arr", "Array(Int32)");
        List<Integer> src = Arrays.asList(1, 2, 3);
        Integer[] dst = JdbcUtils.convertList(src, Integer.class);
        assertEquals(dst.length, src.size());
        assertEquals(dst[0], src.get(0));
        assertEquals(dst[1], src.get(1));
        assertEquals(dst[2], src.get(2));

        assertNull(JdbcUtils.convertList(null, Integer.class));
    }


    @Test(groups = {"unit"})
    public void testConvertToInetAddress() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("ip", "IPv4");
        assertEquals(JdbcUtils.convert(java.net.InetAddress.getByName("192.168.0.1"), java.net.Inet6Address.class, column).toString(), "/0:0:0:0:0:ffff:c0a8:1");
    }

    @DataProvider(name = "timeZones")
    public Object[][] timeZones() {
        return new Object[][] {
                { ZoneId.of("UTC") },
                { ZoneId.of("Europe/Paris") },
        };
    }

    @Test(groups = {"unit"})
    public void testConvertToJavaTimeInstant() throws Exception {
        Instant timestamp = Instant.parse("2016-06-07T21:30:00Z");
        assertEquals(JdbcUtils.convert(timestamp.atZone(ZoneId.of("UTC")), Instant.class), timestamp);
        assertEquals(JdbcUtils.convert(timestamp.atZone(ZoneId.of("UTC+7")), Instant.class), timestamp);
    }
}
