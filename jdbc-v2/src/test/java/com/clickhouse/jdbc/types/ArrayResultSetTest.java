package com.clickhouse.jdbc.types;

import com.clickhouse.data.ClickHouseColumn;
import org.testng.annotations.Test;

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
}