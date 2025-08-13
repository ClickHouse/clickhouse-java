package com.clickhouse.jdbc.internal;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

import static org.testng.Assert.assertEquals;

public class JdbcUtilsTest {


    @Test(groups = {"unit"})
    public void testConvertPrimitiveTypes() throws SQLException {
        assertEquals(JdbcUtils.convert(1, int.class), 1);
        assertEquals(JdbcUtils.convert(1L, long.class), 1L);
        assertEquals(JdbcUtils.convert("1", String.class), "1");
        assertEquals(JdbcUtils.convert(1.0f, float.class), 1.0f);
        assertEquals(JdbcUtils.convert(1.0, double.class), 1.0);
        assertEquals(JdbcUtils.convert(true, boolean.class), true);
        assertEquals(JdbcUtils.convert((short) 1, short.class), (short) 1);
        assertEquals(JdbcUtils.convert((byte) 1, byte.class), (byte) 1);
        assertEquals(JdbcUtils.convert(1.0d, BigDecimal.class), BigDecimal.valueOf(1.0d));
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
        assertEquals(arr.getClass().getComponentType(), Object.class);
        Object[] arrs = (Object[]) arr;
        assertEquals(arrs[0], 1);
        assertEquals(arrs[1], 2);
    }
}
