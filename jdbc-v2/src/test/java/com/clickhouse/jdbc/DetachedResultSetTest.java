package com.clickhouse.jdbc;

import com.clickhouse.client.api.DataTypeUtils;
import com.clickhouse.jdbc.internal.DetachedResultSet;
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
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class DetachedResultSetTest extends JdbcIntegrationTest {

    private static Calendar defaultCalendar = Calendar.getInstance();

    @Test(groups = "integration")
    public void shouldReturnColumnIndex() throws SQLException {
        runQuery("CREATE TABLE detached_rs_test_data (id UInt32, val UInt8) ENGINE = MergeTree ORDER BY (id)");
        runQuery("INSERT INTO detached_rs_test_data VALUES (1, 10), (2, 20)");

        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet srcRs = stmt.executeQuery("SELECT * FROM detached_rs_test_data ORDER BY id")) {
                    ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
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

        try (Connection conn = this.getJdbcConnection(); Statement stmt = conn.createStatement();
             ResultSet srcRs = stmt.executeQuery("SELECT 1")) {
            ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
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
                    () -> rs.updateObject(1, 1),
                    () -> rs.updateObject("col1", 1),
                    () -> rs.updateObject(1, "test", Types.INTEGER),
                    () -> rs.updateObject("col1", "test", Types.INTEGER),

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
                op.run();
            }
        }
    }


    @Test(groups = {"integration"})
    public void testCursorPosition() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet srcRs = stmt.executeQuery("select number from system.numbers LIMIT 2")) {
                ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
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
            try (ResultSet srcRs = stmt.executeQuery("select number from system.numbers LIMIT 2")) {
                ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
                Assert.assertEquals(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
                Assert.expectThrows(SQLException.class, () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
                Assert.expectThrows(SQLException.class, () -> rs.setFetchDirection(ResultSet.FETCH_UNKNOWN));
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);

                Assert.assertEquals(rs.getFetchSize(), 0);
                rs.setFetchSize(10);
                Assert.assertEquals(rs.getFetchSize(), 0);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testConstants() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet srcRs = stmt.executeQuery("select number from system.numbers LIMIT 2")) {
                ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
                Assert.assertNull(rs.getStatement());
                Assert.assertEquals(rs.getType(), ResultSet.TYPE_FORWARD_ONLY);
                Assert.assertEquals(rs.getConcurrency(), ResultSet.CONCUR_READ_ONLY);
                Assert.assertEquals(rs.getHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
                assertFalse(rs.isClosed());
                rs.close();
                assertTrue(rs.isClosed());
                assertThrows(SQLException.class, rs::next);
                assertThrows(SQLException.class, rs::getStatement);
                assertThrows(SQLException.class, rs::getMetaData);
            }
        }
    }

    @Test(groups = {"integration"})
    public void testWasNull() throws SQLException {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            final String sql = "select NULL::Nullable(%s) as v1";

            try (ResultSet srcRs = stmt.executeQuery(sql.formatted("Int64"))) {
                ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
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
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet srcRs = stmt.executeQuery("select '1'::Int32 as v1, 'test' as v2 ")) {
                ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
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

    @Test(groups = { "integration" })
    public void testDateTimeTypes() throws SQLException {
        runQuery("CREATE TABLE detached_rs_test_dates (order Int8, "
                + "date Date, date32 Date32, " +
                "dateTime DateTime, dateTime32 DateTime32, " +
                "dateTime643 DateTime64(3), dateTime646 DateTime64(6), dateTime649 DateTime64(9)"
                + ") ENGINE = MergeTree ORDER BY ()");

        // Insert minimum values
        insertData("INSERT INTO detached_rs_test_dates VALUES ( 1, '1970-01-01', '1970-01-01', " +
                "'1970-01-01 00:00:00', '1970-01-01 00:00:00', " +
                "'1970-01-01 00:00:00.000', '1970-01-01 00:00:00.000000', '1970-01-01 00:00:00.000000000' )");

        // Insert maximum values
        insertData("INSERT INTO detached_rs_test_dates VALUES ( 2, '2149-06-06', '2299-12-31', " +
                "'2106-02-07 06:28:15', '2106-02-07 06:28:15', " +
                "'2261-12-31 23:59:59.999', '2261-12-31 23:59:59.999999', '2261-12-31 23:59:59.999999999' )");

        // Insert random (valid) values
        final ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        final LocalDateTime now = LocalDateTime.now(zoneId);
        final Date date = Date.valueOf(now.toLocalDate());
        final Date date32 = Date.valueOf(now.toLocalDate());
        final java.sql.Timestamp dateTime = Timestamp.valueOf(now);
        dateTime.setNanos(0);
        final java.sql.Timestamp dateTime32 = Timestamp.valueOf(now);
        dateTime32.setNanos(0);
        final java.sql.Timestamp dateTime643 = Timestamp.valueOf(LocalDateTime.now(ZoneId.of("America/Los_Angeles")));
        dateTime643.setNanos(333000000);
        final java.sql.Timestamp dateTime646 = Timestamp.valueOf(LocalDateTime.now(ZoneId.of("America/Los_Angeles")));
        dateTime646.setNanos(333333000);
        final java.sql.Timestamp dateTime649 = Timestamp.valueOf(LocalDateTime.now(ZoneId.of("America/Los_Angeles")));
        dateTime649.setNanos(333333333);

        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO detached_rs_test_dates VALUES ( 4, ?, ?, ?, ?, ?, ?, ?)")) {
                stmt.setDate(1, date);
                stmt.setDate(2, date32);
                stmt.setTimestamp(3, dateTime);
                stmt.setTimestamp(4, dateTime32);
                stmt.setTimestamp(5, dateTime643);
                stmt.setTimestamp(6, dateTime646);
                stmt.setTimestamp(7, dateTime649);
                stmt.executeUpdate();
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet srcRs = stmt.executeQuery("SELECT * FROM detached_rs_test_dates ORDER BY order")) {
                    ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getDate("date32"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getTimestamp("dateTime").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime32").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime643").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime646").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getTimestamp("dateTime649").toString(), "1970-01-01 00:00:00.0");

                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date"), Date.valueOf("2149-06-06"));
                    assertEquals(rs.getDate("date32"), Date.valueOf("2299-12-31"));
                    assertEquals(rs.getTimestamp("dateTime").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getTimestamp("dateTime32").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getTimestamp("dateTime643").toString(), "2261-12-31 23:59:59.999");
                    assertEquals(rs.getTimestamp("dateTime646").toString(), "2261-12-31 23:59:59.999999");
                    assertEquals(rs.getTimestamp("dateTime649").toString(), "2261-12-31 23:59:59.999999999");

                    assertTrue(rs.next());
                    assertEquals(rs.getDate("date").toString(), date.toString());
                    assertEquals(rs.getDate("date32").toString(), date32.toString());
                    assertEquals(rs.getTimestamp("dateTime").toString(), Timestamp.valueOf(dateTime.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime32").toString(), Timestamp.valueOf(dateTime32.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime643").toString(), Timestamp.valueOf(dateTime643.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime646").toString(), Timestamp.valueOf(dateTime646.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getTimestamp("dateTime649").toString(), Timestamp.valueOf(dateTime649.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());

                    assertEquals(rs.getTimestamp("dateTime", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime.toString());
                    assertEquals(rs.getTimestamp("dateTime32", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime32.toString());
                    assertEquals(rs.getTimestamp("dateTime643", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime643.toString());
                    assertEquals(rs.getTimestamp("dateTime646", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime646.toString());
                    assertEquals(rs.getTimestamp("dateTime649", new GregorianCalendar(TimeZone.getTimeZone("UTC"))).toString(), dateTime649.toString());

                    assertFalse(rs.next());
                }
            }
        }

        // Check the results
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet srcRs = stmt.executeQuery("SELECT * FROM detached_rs_test_dates ORDER BY order")) {
                    ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
                    assertTrue(rs.next());
                    assertEquals(rs.getObject("date"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getObject("date32"), Date.valueOf("1970-01-01"));
                    assertEquals(rs.getObject("dateTime").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime32").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime643").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime646").toString(), "1970-01-01 00:00:00.0");
                    assertEquals(rs.getObject("dateTime649").toString(), "1970-01-01 00:00:00.0");

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("date"), Date.valueOf("2149-06-06"));
                    assertEquals(rs.getObject("date32"), Date.valueOf("2299-12-31"));
                    assertEquals(rs.getObject("dateTime").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getObject("dateTime32").toString(), "2106-02-07 06:28:15.0");
                    assertEquals(rs.getObject("dateTime643").toString(), "2261-12-31 23:59:59.999");
                    assertEquals(rs.getObject("dateTime646").toString(), "2261-12-31 23:59:59.999999");
                    assertEquals(rs.getObject("dateTime649").toString(), "2261-12-31 23:59:59.999999999");

                    assertTrue(rs.next());
                    assertEquals(rs.getObject("date").toString(), date.toString());
                    assertEquals(rs.getObject("date32").toString(), date32.toString());

                    assertEquals(rs.getObject("dateTime").toString(), Timestamp.valueOf(dateTime.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime32").toString(), Timestamp.valueOf(dateTime32.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime643").toString(), Timestamp.valueOf(dateTime643.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime646").toString(), Timestamp.valueOf(dateTime646.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());
                    assertEquals(rs.getObject("dateTime649").toString(), Timestamp.valueOf(dateTime649.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime()).toString());

                    assertFalse(rs.next());
                }
            }
        }

        try (Connection conn = getJdbcConnection();
             Statement stmt = conn.createStatement();
             ResultSet srcRs = stmt.executeQuery("SELECT * FROM detached_rs_test_dates ORDER BY order"))
        {
            ResultSet rs = DetachedResultSet.createFromResultSet(srcRs, defaultCalendar, Collections.emptyList());
            assertTrue(rs.next());
            assertEquals(rs.getString("date"), "1970-01-01");
            assertEquals(rs.getString("date32"), "1970-01-01");
            assertEquals(rs.getString("dateTime"), "1970-01-01 00:00:00.0");
            assertEquals(rs.getString("dateTime32"), "1970-01-01 00:00:00.0");
            assertEquals(rs.getString("dateTime643"), "1970-01-01 00:00:00.0");
            assertEquals(rs.getString("dateTime646"), "1970-01-01 00:00:00.0");
            assertEquals(rs.getString("dateTime649"), "1970-01-01 00:00:00.0");

            assertTrue(rs.next());
            assertEquals(rs.getString("date"), "2149-06-06");
            assertEquals(rs.getString("date32"), "2299-12-31");
            assertEquals(rs.getString("dateTime"), "2106-02-07 06:28:15.0");
            assertEquals(rs.getString("dateTime32"), "2106-02-07 06:28:15.0");
            assertEquals(rs.getString("dateTime643"), "2261-12-31 23:59:59.999");
            assertEquals(rs.getString("dateTime646"), "2261-12-31 23:59:59.999999");
            assertEquals(rs.getString("dateTime649"), "2261-12-31 23:59:59.999999999");

            ZoneId tzServer = ZoneId.of(((ConnectionImpl) conn).getClient().getServerTimeZone());
            assertTrue(rs.next());
            assertEquals(
                    rs.getString("date"),
                    Instant.ofEpochMilli(date.getTime()).atZone(tzServer).toLocalDate().toString());
            assertEquals(
                    rs.getString("date32"),
                    Instant.ofEpochMilli(date32.getTime()).atZone(tzServer).toLocalDate().toString());
            assertFalse(rs.next());
        }
    }

    private int insertData(String sql) throws SQLException {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                return stmt.executeUpdate(sql);
            }
        }
    }
}
