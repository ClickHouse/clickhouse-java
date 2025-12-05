package com.clickhouse.jdbc.types;

import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.testng.Assert.*;

@Test(groups = {"unit"})
public class ArrayResultSetTest {

    @Test
    void testCursorNavigation() throws SQLException {
        Integer[] array = {1, 2, 3, 4, 5};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Int32)").get(0));

        assertEquals(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
        rs.setFetchDirection(ResultSet.FETCH_REVERSE);
        assertEquals(rs.getFetchDirection(), ResultSet.FETCH_REVERSE);
        assertThrows(SQLException.class, () -> rs.setFetchDirection(123));
        assertEquals(rs.getType(), ResultSet.TYPE_SCROLL_INSENSITIVE);
        assertEquals(rs.getConcurrency(), ResultSet.CONCUR_READ_ONLY);
        rs.setFetchSize(10000);
        assertEquals(rs.getFetchSize(), 10000);
        assertEquals(rs.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertFalse(rs.isClosed());

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertThrows(SQLException.class, () -> rs.getShort(2));

        rs.next();

        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.isFirst());
        assertFalse(rs.isLast());

        assertEquals(rs.getShort(1), 1); // INDEX
        assertEquals(rs.getShort(2), array[0].shortValue()); // VALUE
        assertEquals(rs.getInt(1), rs.getRow());

        assertTrue(rs.relative(2));
        assertEquals(rs.getRow(), 3); // INDEX
        assertEquals(rs.getShort(2), array[2].shortValue()); // VALUE

        assertTrue(rs.previous());
        assertEquals(rs.getRow(), 2); // INDEX
        assertEquals(rs.getShort(2), array[1].shortValue()); // VALUE


        assertTrue(rs.absolute(array.length)); // INDEX - last element
        assertEquals(rs.getRow(), array.length); // INDEX
        assertEquals(rs.getShort(2), array[array.length - 1].shortValue()); // VALUE
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertTrue(rs.isLast());


        assertFalse(rs.relative(2));
        assertEquals(rs.getRow(), 0); // INDEX
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());

        rs.first();
        assertEquals(rs.getRow(), 1); // INDEX
        assertEquals(rs.getShort(2), array[0].shortValue()); // VALUE
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.isFirst());
        assertFalse(rs.isLast());

        rs.last();
        assertEquals(rs.getRow(), array.length); // INDEX
        assertEquals(rs.getShort(2), array[array.length - 1].shortValue()); // VALUE
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertTrue(rs.isLast());

        rs.beforeFirst();
        assertEquals(rs.getRow(), 0); // INDEX
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());

        rs.afterLast();
        assertEquals(rs.getRow(), 0); // INDEX
        assertTrue(rs.isAfterLast());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());

        assertFalse(rs.next());
        assertThrows(SQLException.class, () -> rs.getShort(2));

        rs.close();
        assertTrue(rs.isClosed());
    }

    @Test
    void testNullValues() throws SQLException {
        Integer[] array = {1, null, 3, 4, 5};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Int32)").get(0));

        rs.next();
        assertFalse(rs.wasNull());
        assertEquals(rs.getInt(2), array[0]);
        assertFalse(rs.wasNull());

        rs.next();
        assertFalse(rs.wasNull());
        assertEquals(rs.getInt(2), 0);
        assertTrue(rs.wasNull());
    }

    @Test(dataProvider = "testPrimitiveValues")
    void testPrimitiveValues(Object array, ClickHouseColumn column) throws SQLException {
        ArrayResultSet rs = new ArrayResultSet(array, column);

        int len = java.lang.reflect.Array.getLength(array);
        Class<?> itemClass = array.getClass().getComponentType();
        for (int i = 0; i < len; i++) {
            rs.next();
            Object value = Array.get(array, i);
            Object actualValue = rs.getObject(2);
            assertEquals(actualValue, value, "Actual value is " + actualValue.getClass() + " but expected " + value.getClass());
            if (itemClass.isPrimitive() && (itemClass != Boolean.class && itemClass != boolean.class)) {
                assertEquals(rs.getByte(2), ((Number) value).byteValue());
                assertEquals(rs.getShort(2), ((Number) value).shortValue());
                assertEquals(rs.getInt(2), ((Number) value).intValue());
                assertEquals(rs.getLong(2), ((Number) value).longValue());
                assertEquals(rs.getFloat(2), ((Number) value).floatValue());
                assertEquals(rs.getDouble(2), ((Number) value).doubleValue());
            } else if (Number.class.isAssignableFrom(itemClass)) {
                assertEquals(rs.getByte(2), ((Number) value).byteValue());
                assertEquals(rs.getShort(2), ((Number) value).shortValue());
                assertEquals(rs.getInt(2), ((Number) value).intValue());
                assertEquals(rs.getLong(2), ((Number) value).longValue());
                assertEquals(rs.getFloat(2), ((Number) value).floatValue());
                assertEquals(rs.getDouble(2), ((Number) value).doubleValue());
            } else if (itemClass == Boolean.class || itemClass == boolean.class) {
                Number number = ((Boolean) value) ? 1 : 0;
                assertEquals(rs.getByte(2), number.byteValue());
                assertEquals(rs.getShort(2), number.shortValue());
                assertEquals(rs.getInt(2), number.intValue());
                assertEquals(rs.getLong(2), number.longValue());
                assertEquals(rs.getFloat(2), number.floatValue());
                assertEquals(rs.getDouble(2), number.doubleValue());
            }
        }
    }

    @DataProvider
    static Object[][] testPrimitiveValues() {
        return new Object[][]{
                {(Object) new boolean[]{true, false, true}, ClickHouseColumn.parse("v Array(Bool)").get(0)},
                {(Object) new byte[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int8)").get(0)},
                {(Object) new short[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int16)").get(0)},
                {(Object) new int[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int32)").get(0)},
                {(Object) new long[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int64)").get(0)},
                {(Object) new double[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Float64)").get(0)},
                {(Object) new float[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Float32)").get(0)},
                {(Object) new String[]{"a", "b", "c"}, ClickHouseColumn.parse("v Array(String)").get(0)},
                {(Object) new Boolean[]{true, false, true}, ClickHouseColumn.parse("v Array(Bool)").get(0)},
                {(Object) new Byte[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int8)").get(0)},
                {(Object) new Short[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int16)").get(0)},
                {(Object) new Integer[]{1, 2, 3}, ClickHouseColumn.parse("v Array(Int32)").get(0)},
                {(Object) new Long[]{1L, 2L, 3L}, ClickHouseColumn.parse("v Array(Int64)").get(0)},
                {(Object) new Float[]{1.0F, 2.0F, 3.0F}, ClickHouseColumn.parse("v Array(Float32)").get(0)},
                {(Object) new Double[]{1.0D, 2.0D, 3.0D}, ClickHouseColumn.parse("v Array(Float64)").get(0)},
                {(Object) new String[]{"a", "b", "c"}, ClickHouseColumn.parse("v Array(String)").get(0)},
        };
    }

    @Test(dataProvider = "testPrimitiveMultiDimensionalValues")
    void testPrimitiveMultiDimensionalValues(Object array, ClickHouseColumn column) throws SQLException {
        ArrayResultSet rs = new ArrayResultSet(array, column);

        int len = java.lang.reflect.Array.getLength(array);
        Class<?> itemClass = array.getClass().getComponentType();
        for (int i = 0; i < len; i++) {
            rs.next();
            Object value = Array.get(array, i);
            java.sql.Array sqlArray = (java.sql.Array) rs.getObject(2);
            assertEquals(sqlArray.getArray(), value);

            ArrayResultSet nestedRs = (ArrayResultSet) sqlArray.getResultSet();
            for (int j = 0; j < len; j++) {
                nestedRs.next();
                Object nestedValue = Array.get(value, j);
                assertEquals(nestedRs.getObject(2), nestedValue);
            }
        }
    }

    @DataProvider
    static Object[][] testPrimitiveMultiDimensionalValues() {
        return new Object[][]{
                {(Object) new boolean[][]{new boolean[]{true, false, true}, new boolean[]{true, false, true}}, ClickHouseColumn.parse("v Array(Array(Bool))").get(0)},
                {(Object) new byte[][]{new byte[]{1, 2, 3}, new byte[]{1, 2, 3}}, ClickHouseColumn.parse("v Array(Array(Int8))").get(0)},
                {(Object) new short[][]{new short[]{1, 2, 3}, new short[]{1, 2, 3}}, ClickHouseColumn.parse("v Array(Array(Int16))").get(0)},

                {(Object) new int[][]{new int[]{1, 2, 3}, new int[]{1, 2, 3}}, ClickHouseColumn.parse("v Array(Array(Int32))").get(0)},
                {(Object) new long[][]{new long[]{1, 2, 3}, new long[]{1, 2, 3}}, ClickHouseColumn.parse("v Array(Array(Int64))").get(0)},
                {(Object) new float[][]{new float[]{1, 2, 3}, new float[]{1, 2, 3}}, ClickHouseColumn.parse("v Array(Array(Float32))").get(0)},
                {(Object) new double[][]{new double[]{1, 2, 3}, new double[]{1, 2, 3}}, ClickHouseColumn.parse("v Array(Array(Float64))").get(0)},
        };
    }
}