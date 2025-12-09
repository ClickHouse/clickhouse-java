package com.clickhouse.jdbc.types;

import com.clickhouse.data.ClickHouseColumn;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.GregorianCalendar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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

        assertFalse(rs.rowDeleted());
        assertFalse(rs.rowInserted());
        assertFalse(rs.rowUpdated());

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

        expectThrows(SQLException.class, () -> rs.findColumn("something"));
        String valueColumn = rs.getMetaData().getColumnLabel(2);
        int len = java.lang.reflect.Array.getLength(array);
        Class<?> itemClass = array.getClass().getComponentType();
        for (int i = 0; i < len; i++) {
            rs.next();
            Object value = Array.get(array, i);
            Object actualValue = rs.getObject(valueColumn);
            assertEquals(actualValue, value, "Actual value is " + actualValue.getClass() + " but expected " + value.getClass());
            if (itemClass.isPrimitive() && (itemClass != Boolean.class && itemClass != boolean.class)) {
                Number number = ((Number) value);
                assertEquals(rs.getByte(valueColumn), number.byteValue());
                assertEquals(rs.getShort(valueColumn), number.shortValue());
                assertEquals(rs.getInt(valueColumn), number.intValue());
                assertEquals(rs.getLong(valueColumn), number.longValue());
                assertEquals(rs.getFloat(valueColumn), number.floatValue());
                assertEquals(rs.getDouble(valueColumn), number.doubleValue());
                assertEquals(rs.getString(valueColumn), String.valueOf(value));
            } else if (Number.class.isAssignableFrom(itemClass)) {
                Number number = ((Number) value);
                assertEquals(rs.getByte(valueColumn), number.byteValue());
                assertEquals(rs.getShort(valueColumn), number.shortValue());
                assertEquals(rs.getInt(valueColumn), number.intValue());
                assertEquals(rs.getLong(valueColumn), number.longValue());
                assertEquals(rs.getFloat(valueColumn), number.floatValue());
                assertEquals(rs.getDouble(valueColumn), number.doubleValue());
                assertEquals(rs.getBigDecimal(valueColumn), BigDecimal.valueOf(number.doubleValue()));
            } else if (itemClass == Boolean.class || itemClass == boolean.class) {
                Number number = ((Boolean) value) ? 1 : 0;
                assertEquals(rs.getBoolean(valueColumn), ((Boolean) value));
                assertEquals(rs.getByte(valueColumn), number.byteValue());
                assertEquals(rs.getShort(valueColumn), number.shortValue());
                assertEquals(rs.getInt(valueColumn), number.intValue());
                assertEquals(rs.getLong(valueColumn), number.longValue());
                assertEquals(rs.getFloat(valueColumn), number.floatValue());
                assertEquals(rs.getDouble(valueColumn), number.doubleValue());
            }

            String indexColumn = rs.getMetaData().getColumnName(1);
            assertEquals(rs.getByte(indexColumn), i + 1);
            assertEquals(rs.getShort(indexColumn), i + 1);
            assertEquals(rs.getInt(indexColumn), i + 1);
            assertEquals(rs.getLong(indexColumn), i + 1);
            assertEquals(rs.getFloat(indexColumn), i + 1);
            assertEquals(rs.getDouble(indexColumn), i + 1);
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
        final String valueColumn = rs.getMetaData().getColumnName(2);
        for (int i = 0; i < len; i++) {
            rs.next();
            Object value = Array.get(array, i);
            java.sql.Array sqlArray = (java.sql.Array) rs.getObject(valueColumn);
            assertEquals(sqlArray.getArray(), value);
            java.sql.Array sqlArray2 = rs.getArray(valueColumn);
            assertEquals(sqlArray2.getArray(), value);

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

    @Test
    void testStringValues() throws SQLException {
        String[] array = new String[] {"a", null, "c"};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Nullable(String))").get(0));

        final String valueColumn = rs.getMetaData().getColumnLabel(2);
        int len = java.lang.reflect.Array.getLength(array);
        for (int i = 0; i < len; i++) {
            rs.next();
            String value = array[i];
            assertEquals(rs.getString(valueColumn), value);
            assertEquals(rs.getObject(valueColumn), value);
            assertEquals(rs.getNString(valueColumn), value);
            if (value == null) {
                assertTrue(rs.wasNull());
            } else {
                assertEquals(rs.getBytes(valueColumn), value.getBytes());
            }
        }
    }

    @Test
    void testEmptyArray() throws SQLException {
        ArrayResultSet rs = new ArrayResultSet(new Object[0], ClickHouseColumn.parse("v Array(Int32)").get(0));
        assertFalse(rs.next());
    }

    @Test
    public void testReadOnlyException() throws Throwable {
        Integer[] array = {1, null, 3, 4, 5};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Int32)").get(0));

        rs.next();
        final String valueColumn = rs.getMetaData().getColumnName(2);
        Assert.ThrowingRunnable[] rsUnsupportedMethods = new Assert.ThrowingRunnable[]{
                rs::moveToCurrentRow,
                rs::moveToInsertRow,
                rs::refreshRow,
                () -> rs.updateBoolean("col1", true),
                () -> rs.updateByte("col1", (byte) 1),
                () -> rs.updateShort("col1", (short) 1),
                () -> rs.updateInt("col1", 1),
                () -> rs.updateLong("col1", 1L),
                () -> rs.updateFloat("col1", 1.1f),
                () -> rs.updateDouble("col1", 1.1),
                () -> rs.updateBigDecimal("col1", BigDecimal.valueOf(1.1)),
                () -> rs.updateString("col1", "test"),
                () -> rs.updateNString("col1", "test"),
                () -> rs.updateBytes("col1", new byte[1]),
                () -> rs.updateDate("col1", Date.valueOf("2020-01-01")),
                () -> rs.updateTime("col1", Time.valueOf("12:34:56")),
                () -> rs.updateTimestamp("col1", Timestamp.valueOf("2020-01-01 12:34:56.789123")),
                () -> rs.updateBlob("col1", (Blob) null),
                () -> rs.updateClob("col1", new StringReader("test")),
                () -> rs.updateNClob("col1", new StringReader("test")),
                () -> rs.updateBoolean(1, true),
                () -> rs.updateByte(1, (byte) 1),
                () -> rs.updateShort(1, (short) 1),
                () -> rs.updateInt(1, 1),
                () -> rs.updateLong(1, 1L),
                () -> rs.updateFloat(1, 1.1f),
                () -> rs.updateDouble(1, 1.1),
                () -> rs.updateBigDecimal(1, BigDecimal.valueOf(1.1)),
                () -> rs.updateString(1, "test"),
                () -> rs.updateNString(1, "test"),
                () -> rs.updateBytes(1, new byte[1]),
                () -> rs.updateDate(1, Date.valueOf("2020-01-01")),
                () -> rs.updateTime(1, Time.valueOf("12:34:56")),
                () -> rs.updateTimestamp(1, Timestamp.valueOf("2020-01-01 12:34:56.789123")),
                () -> rs.updateBlob(1, (Blob) null),
                () -> rs.updateClob(1, new StringReader("test")),
                () -> rs.updateNClob(1, new StringReader("test")),
                () -> rs.updateSQLXML(1, null),
                () -> rs.updateObject(1, 1),
                () -> rs.updateObject("col1", 1),
                () -> rs.updateObject(1, "test", Types.INTEGER),
                () -> rs.updateObject("col1", "test", Types.INTEGER),
                () -> rs.updateObject(1, "test", JDBCType.INTEGER),
                () -> rs.updateObject("col1", "test", JDBCType.INTEGER),
                () -> rs.updateObject(1, "test", JDBCType.INTEGER, 1),
                () -> rs.updateCharacterStream(1, new StringReader("test"), 1),
                () -> rs.updateCharacterStream("col1", new StringReader("test")),
                () -> rs.updateCharacterStream("col1", new StringReader("test"), 1),
                () -> rs.updateCharacterStream(1, new StringReader("test"), 1L),
                () -> rs.updateCharacterStream("col1", new StringReader("test"), 1L),
                () -> rs.updateCharacterStream(1, new StringReader("test")),
                () -> rs.updateCharacterStream("col1", new StringReader("test")),
                () -> rs.updateNCharacterStream(1, new StringReader("test"), 1),
                () -> rs.updateNCharacterStream("col1", new StringReader("test"), 1),
                () -> rs.updateNCharacterStream(1, new StringReader("test"), 1L),
                () -> rs.updateNCharacterStream("col1", new StringReader("test"), 1L),
                () -> rs.updateNCharacterStream(1, new StringReader("test")),
                () -> rs.updateNCharacterStream("col1", new StringReader("test")),
                () -> rs.updateBlob(1, (InputStream) null),
                () -> rs.updateBlob("col1", (InputStream) null),
                () -> rs.updateBlob(1, (InputStream) null, -1),
                () -> rs.updateBlob("col1", (InputStream) null, -1),
                () -> rs.updateBinaryStream(1, (InputStream) null),
                () -> rs.updateBinaryStream("col1", (InputStream) null),
                () -> rs.updateBinaryStream(1, (InputStream) null, -1),
                () -> rs.updateBinaryStream("col1", (InputStream) null, -1),
                () -> rs.updateBinaryStream(1, (InputStream) null, -1L),
                () -> rs.updateBinaryStream("col1", (InputStream) null, -1L),
                () -> rs.updateAsciiStream(1, (InputStream) null),
                () -> rs.updateAsciiStream("col1", (InputStream) null),
                () -> rs.updateAsciiStream(1, (InputStream) null, -1),
                () -> rs.updateAsciiStream("col1", (InputStream) null, -1),
                () -> rs.updateAsciiStream(1, (InputStream) null, -1L),
                () -> rs.updateAsciiStream("col1", (InputStream) null, -1L),
                () -> rs.updateClob(1, (Reader) null),
                () -> rs.updateClob("col1", (Reader) null),
                () -> rs.updateClob(1, (Reader) null, -1),
                () -> rs.updateClob("col1", (Reader) null, -1),
                () -> rs.updateClob(1, (Reader) null, -1L),
                () -> rs.updateClob("col1", (Reader) null, -1L),
                () -> rs.updateNClob(1, (Reader) null),
                () -> rs.updateNClob("col1", (Reader) null),
                () -> rs.updateNClob(1, (NClob) null),
                () -> rs.updateNClob("col1", (NClob) null),
                () -> rs.updateNClob(1, (Reader) null, -1),
                () -> rs.updateNClob("col1", (Reader) null, -1),
                () -> rs.updateNClob(1, (Reader) null, -1L),
                () -> rs.updateNClob("col1", (Reader) null, -1L),
                () -> rs.updateRef(1, (Ref) null),
                () -> rs.updateRef("col1", (Ref) null),
                () -> rs.updateArray(1, (java.sql.Array) null),
                () -> rs.updateArray("col1", (java.sql.Array) null),
                rs::cancelRowUpdates,
                () -> rs.updateNull(1),
                () -> rs.updateNull("col1"),
                () -> rs.updateRowId(1, null),
                () -> rs.updateRowId("col1", null),
                () -> rs.updateClob(1, (Clob) null),
                () -> rs.updateClob("col1", (Clob) null),
                rs::updateRow,
                rs::insertRow,
                rs::deleteRow,
                () -> rs.getCharacterStream(valueColumn),
                () -> rs.getBinaryStream(valueColumn),
                () -> rs.getUnicodeStream(valueColumn),
                () -> rs.getAsciiStream(valueColumn),
                () -> rs.getNCharacterStream(valueColumn),
                () -> rs.getNClob(valueColumn),
                () -> rs.getClob(valueColumn),
                () -> rs.getBlob(valueColumn),
                () -> rs.getSQLXML(valueColumn),
                () -> rs.getRef(valueColumn),
                () -> rs.getRowId(valueColumn),
                () -> rs.getSQLXML(valueColumn),
        };

        for (Assert.ThrowingRunnable op : rsUnsupportedMethods) {
            Assert.assertThrows(SQLException.class, op);
        }
    }

    @Test
    void testIndexColumn() throws Exception {
        Integer[] array = {1, null, 3, 4, 5};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Int32)").get(0));

        final String indexColumn = rs.getMetaData().getColumnName(1);
        Assert.assertEquals(indexColumn, "INDEX");
        rs.next();
        assertEquals(rs.getObject(indexColumn), 1);
        assertEquals(rs.getObject(indexColumn, String.class), "1");
        assertEquals(rs.getObject(indexColumn, Long.class), 1L);
        assertEquals(rs.getObject(indexColumn, Integer.class), 1);
        assertEquals(rs.getObject(indexColumn, Short.class), (short) 1);
        assertEquals(rs.getObject(indexColumn, Byte.class), (byte) 1);
    }

    @Test
    void testDateColumn() throws Exception {
        Date[] array = {Date.valueOf("2020-01-01"), null, Date.valueOf("2020-01-02")};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Date)").get(0));

        final String dateColumn = rs.getMetaData().getColumnName(2);
        Assert.assertEquals(dateColumn, "VALUE");
        rs.next();
        assertEquals(rs.getObject(dateColumn), Date.valueOf("2020-01-01"));
        assertEquals(rs.getObject(dateColumn, String.class), "2020-01-01");
        assertEquals(rs.getDate(dateColumn), rs.getObject(dateColumn, Date.class));
        assertEquals(rs.getDate(dateColumn, new GregorianCalendar()), rs.getObject(dateColumn, Date.class));
    }

    @Test
    void testTimeColumn() throws Exception {
        Time[] array = {Time.valueOf("12:34:56"), null, Time.valueOf("12:34:57")};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Time)").get(0));

        final String timeColumn = rs.getMetaData().getColumnName(2);
        Assert.assertEquals(timeColumn, "VALUE");
        rs.next();
        assertEquals(rs.getObject(timeColumn), Time.valueOf("12:34:56"));
        assertEquals(rs.getObject(timeColumn, String.class), "12:34:56");
        assertEquals(rs.getTime(timeColumn), rs.getObject(timeColumn, Time.class));
        assertEquals(rs.getTime(timeColumn, new GregorianCalendar()), rs.getObject(timeColumn, Time.class));
    }

    @Test
    void testTimestampColumn() throws Exception {
        Timestamp[] array = {Timestamp.valueOf("2020-01-01 12:34:56.789123"), null, Timestamp.valueOf("2020-01-01 12:34:56.789124")};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(Timestamp)").get(0));

        final String timestampColumn = rs.getMetaData().getColumnName(2);
        Assert.assertEquals(timestampColumn, "VALUE");
        rs.next();
        assertEquals(rs.getObject(timestampColumn), Timestamp.valueOf("2020-01-01 12:34:56.789123"));
        assertEquals(rs.getObject(timestampColumn, String.class), "2020-01-01 12:34:56.789123");
        assertEquals(rs.getTimestamp(timestampColumn), rs.getObject(timestampColumn, Timestamp.class));
        assertEquals(rs.getTimestamp(timestampColumn, new GregorianCalendar()), rs.getObject(timestampColumn, Timestamp.class));
    }

    @Test
    void testStringColumn() throws Exception {
        String[] array = {"123", null, "456"};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(String)").get(0));

        final String stringColumn = rs.getMetaData().getColumnName(2);
        Assert.assertEquals(stringColumn, "VALUE");
        rs.next();
        assertEquals(rs.getObject(stringColumn), "123");
        assertEquals(rs.getObject(stringColumn, String.class), "123");
        assertEquals(rs.getString(stringColumn), rs.getObject(stringColumn, String.class));
        assertEquals(rs.getByte(stringColumn), 123);
        assertEquals(rs.getShort(stringColumn), (short) 123);
        assertEquals(rs.getInt(stringColumn), 123);
        assertEquals(rs.getLong(stringColumn), 123L);
        assertEquals(rs.getFloat(stringColumn), 123.0f);
        assertEquals(rs.getDouble(stringColumn), 123.0);
    }

    @Test
    void testStringToURL() throws Exception {
        String[] array = {"http://test.com", null, "https://test.com"};
        ArrayResultSet rs = new ArrayResultSet(array, ClickHouseColumn.parse("v Array(String)").get(0));

        final String stringColumn = rs.getMetaData().getColumnName(2);
        Assert.assertEquals(stringColumn, "VALUE");
        rs.next();
        assertEquals(rs.getObject(stringColumn), "http://test.com");
        assertEquals(rs.getObject(stringColumn, String.class), "http://test.com");
        assertEquals(rs.getString(stringColumn), rs.getObject(stringColumn, String.class));
        assertEquals(rs.getURL(stringColumn), rs.getObject(stringColumn, URL.class));
    }
}