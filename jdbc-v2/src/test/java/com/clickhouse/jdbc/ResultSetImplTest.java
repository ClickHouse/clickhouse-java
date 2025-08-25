package com.clickhouse.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ResultSetImplTest extends JdbcIntegrationTest {

    @Test(groups = "integration")
    public void shouldReturnColumnIndex() throws SQLException {
        runQuery("CREATE TABLE rs_test_data (id UInt32, val UInt8) ENGINE = MergeTree ORDER BY (id)");
        runQuery("INSERT INTO rs_test_data VALUES (1, 10), (2, 20)");

        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM rs_test_data ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.findColumn("id"), 1);
                    assertEquals(rs.getInt(1), 1);
                    assertEquals(rs.findColumn("val"), 2);
                    assertEquals(rs.getInt(2), 10);

                    assertTrue(rs.next());
                    assertEquals(rs.findColumn("id"), 1);
                    assertEquals(rs.getInt(1), 2);
                    assertEquals(rs.findColumn("val"), 2);
                    assertEquals(rs.getInt(2), 20);
                }
            }
        }
    }

    @Test(groups = {"integration"})
    public void testUnsupportedOperations() throws Throwable {

        boolean[] throwUnsupportedException = new boolean[]{false, true};

        for (boolean flag : throwUnsupportedException) {
            Properties props = new Properties();
            if (flag) {
                props.setProperty(DriverProperties.IGNORE_UNSUPPORTED_VALUES.getKey(), "true");
            }

            try (Connection conn = this.getJdbcConnection(props); Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                Assert.ThrowingRunnable[] rsUnsupportedMethods = new Assert.ThrowingRunnable[]{
                        rs::first,
                        rs::afterLast,
                        rs::beforeFirst,
                        () -> rs.absolute(-1),
                        () -> rs.relative(-1),
                        rs::moveToCurrentRow,
                        rs::moveToInsertRow,
                        rs::last,
                        rs::previous,
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
                        () -> rs.updateArray(1, (Array) null),
                        () -> rs.updateArray("col1", (Array) null),
                        () -> rs.getSQLXML(1),
                        () -> rs.getSQLXML("col1"),
                        () -> rs.getBlob(1),
                        () -> rs.getBlob("col1"),
                        () -> rs.getClob(1),
                        () -> rs.getClob("col1"),
                        () -> rs.getNClob(1),
                        () -> rs.getNClob("col1"),
                        () -> rs.getRef(1),
                        () -> rs.getRef("col1"),
                        () -> rs.getRowId(1),
                        () -> rs.getRowId("col1"),
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
                        rs::rowDeleted,
                        rs::rowInserted,
                        rs::rowUpdated,
                        rs::getCursorName,
                };

                for (Assert.ThrowingRunnable op : rsUnsupportedMethods) {
                    if (!flag) {
                        Assert.assertThrows(SQLFeatureNotSupportedException.class, op);
                    } else {
                        op.run();
                    }
                }
            }
        }
    }


    @Test(groups = {"integration"})
    public void testCursorPosition() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select number from system.numbers LIMIT 2")) {
                Assert.assertTrue(rs.isBeforeFirst());
                Assert.assertFalse(rs.isAfterLast());
                Assert.assertFalse(rs.isFirst());
                Assert.assertFalse(rs.isLast());
                Assert.assertEquals(rs.getRow(), 0);

                rs.next();

                Assert.assertFalse(rs.isBeforeFirst());
                Assert.assertFalse(rs.isAfterLast());
                Assert.assertTrue(rs.isFirst());
                Assert.assertFalse(rs.isLast());
                Assert.assertEquals(rs.getRow(), 1);

                rs.next();

                Assert.assertFalse(rs.isBeforeFirst());
                Assert.assertFalse(rs.isAfterLast());
                Assert.assertFalse(rs.isFirst());
                Assert.assertTrue(rs.isLast());
                Assert.assertEquals(rs.getRow(), 2);

                rs.next();

                Assert.assertFalse(rs.isBeforeFirst());
                Assert.assertTrue(rs.isAfterLast());
                Assert.assertFalse(rs.isFirst());
                Assert.assertFalse(rs.isLast());
                Assert.assertEquals(rs.getRow(), 0);
            }

            try (ResultSet rs = stmt.executeQuery("select 1 LIMIT 0")) {
                Assert.assertTrue(rs.isBeforeFirst());
                Assert.assertFalse(rs.isAfterLast());
                Assert.assertFalse(rs.isFirst());
                Assert.assertFalse(rs.isLast());
                Assert.assertEquals(rs.getRow(), 0);

                Assert.assertFalse(rs.next());

                Assert.assertFalse(rs.isBeforeFirst()); // we stepped over the end
                Assert.assertTrue(rs.isAfterLast()); // we stepped over the end
                Assert.assertFalse(rs.isFirst());
                Assert.assertFalse(rs.isLast());
                Assert.assertEquals(rs.getRow(), 0);
            }
        }
    }


    @Test(groups = {"integration"})
    public void testFetchDirectionsAndSize() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select number from system.numbers LIMIT 2")) {
                Assert.assertEquals(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
                Assert.expectThrows(SQLException.class, () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
                Assert.expectThrows(SQLException.class, () -> rs.setFetchDirection(ResultSet.FETCH_UNKNOWN));
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);

                Assert.assertEquals(rs.getFetchSize(), 1);
                rs.setFetchSize(10);
                Assert.assertEquals(rs.getFetchSize(), 10);
                Assert.expectThrows(SQLException.class, () -> rs.setFetchSize(-10));
            }
        }
    }

    @Test(groups = {"integration"})
    public void testConstants() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select number from system.numbers LIMIT 2")) {
                Assert.assertSame(rs.getStatement(), stmt);
                Assert.assertEquals(rs.getType(), ResultSet.TYPE_FORWARD_ONLY);
                Assert.assertEquals(rs.getConcurrency(), ResultSet.CONCUR_READ_ONLY);
                Assert.assertEquals(rs.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testWasNull() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String sql = "select NULL::Nullable(%s) as v1";

            try (ResultSet rs = stmt.executeQuery(sql.formatted("Int64"))) {
                rs.next();
                Assert.assertFalse(rs.wasNull());

                Assert.assertEquals(rs.getByte(1), (byte) 0);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getByte("v1"), (byte) 0);
                Assert.assertTrue(rs.wasNull());

                Assert.assertEquals(rs.getShort(1), (short) 0);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getShort("v1"), (short) 0);
                Assert.assertTrue(rs.wasNull());

                Assert.assertEquals(rs.getInt(1), 0);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getInt("v1"), 0);
                Assert.assertTrue(rs.wasNull());

                Assert.assertEquals(rs.getLong(1), 0L);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getLong("v1"), 0L);
                Assert.assertTrue(rs.wasNull());

                Assert.assertNull(rs.getBigDecimal(1));
                Assert.assertTrue(rs.wasNull());
                Assert.assertNull(rs.getBigDecimal("v1"));
                Assert.assertTrue(rs.wasNull());

                Assert.assertEquals(rs.getFloat(1), 0f);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getFloat("v1"), 0f);
                Assert.assertTrue(rs.wasNull());

                Assert.assertEquals(rs.getDouble(1), 0d);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getDouble("v1"), 0d);
                Assert.assertTrue(rs.wasNull());

                Assert.assertEquals(rs.getBoolean(1), false);
                Assert.assertTrue(rs.wasNull());
                Assert.assertEquals(rs.getBoolean("v1"), false);
                Assert.assertTrue(rs.wasNull());
            }
        }
    }

    @Test(groups = {"integration"})
    public void testGetMetadata() throws SQLException {
        try  (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select '1'::Int32 as v1, 'test' as v2 ")) {

                int v1ColumnIndex = rs.findColumn("v1");
                int v2ColumnIndex = rs.findColumn("v2");

                ResultSetMetaData metaData = rs.getMetaData();
                Assert.assertEquals(metaData.getColumnCount(), 2);
                Assert.assertEquals(metaData.getColumnType(v1ColumnIndex), Types.INTEGER);
                Assert.assertEquals(metaData.getColumnType(v2ColumnIndex), Types.VARCHAR);
                Assert.assertEquals(metaData.getColumnTypeName(v1ColumnIndex), "Int32");
                Assert.assertEquals(metaData.getColumnTypeName(v2ColumnIndex), "String");
            }
        }
    }
}
