package com.clickhouse.jdbc;

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class PreparedStatementTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" })
    public void testSetNull() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setNull(1, Types.INTEGER);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetBoolean() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setBoolean(1, true);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetByte() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setByte(1, (byte) 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals((byte) 1, rs.getByte(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetShort() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setShort(1, (short) 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals((short) 1, rs.getShort(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetInt() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setInt(1, 1);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetLong() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setLong(1, 1L);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1L, rs.getLong(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetFloat() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setFloat(1, 1.0f);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1.0f, rs.getFloat(1), 0.0f);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetDouble() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setDouble(1, 1.0);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1.0, rs.getDouble(1), 0.0);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetString() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setString(1, "test");
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("test", rs.getString(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Ignore("Not supported yet")
    @Test(groups = { "integration" })
    public void testSetBytes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setBytes(1, new byte[] { 1, 2, 3 });
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getBytes(1), new byte[] { 1, 2, 3 });
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetDate() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT toDate(?)")) {
                stmt.setDate(1, java.sql.Date.valueOf("2021-01-01"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), java.sql.Date.valueOf("2021-01-01"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetTime() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT toDateTime(?)")) {
                stmt.setTime(1, java.sql.Time.valueOf("12:34:56"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getTime(1).toString(), "12:34:56");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testSetTimestamp() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT toDateTime64(?, 3)")) {
                stmt.setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 01:34:56.456"), new GregorianCalendar(TimeZone.getTimeZone("UTC")));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getTimestamp(1).toString(), "2021-01-01 01:34:56.456");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testBigDecimal() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setBigDecimal(1, java.math.BigDecimal.valueOf(1.0));
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(java.math.BigDecimal.valueOf(1.0), rs.getBigDecimal(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testPrimitiveArrays() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
                stmt.setObject(1, new String[][] {new String[]{"a"}, new String[]{"b"}, new String[]{"c"}});
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    Array a1 = rs.getArray(1);
                    assertNotNull(a1);
                    assertEquals(Arrays.deepToString((Object[]) a1.getArray()), "[[a], [b], [c]]");
                    Array a2 = rs.getObject(1, Array.class);
                    assertNotNull(a2);
                    assertEquals(Arrays.deepToString((Object[]) a2.getArray()), "[[a], [b], [c]]");
                    Array a3 = rs.getObject(1) instanceof Array ? (Array) rs.getObject(1) : null;
                    assertNotNull(a3);
                    assertEquals(Arrays.deepToString((Object[]) a3.getArray()), "[[a], [b], [c]]");
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = { "integration" })
    public void testEscapeStrings() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("SELECT FALSE OR ? = 'test', ?")) {
                stmt.setString(1, "test\\' OR 1 = 1 --");
                stmt.setString(2, "test\\\\' OR 1 = 1 --");
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "false");
                    assertEquals(rs.getString(2), "test\\\\' OR 1 = 1 --");
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test(groups = "integration")
    void testWithClause() throws Exception {
        int count = 0;
        try (Connection conn = getJdbcConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("with data as (SELECT number FROM numbers(100)) select * from data ")) {
                stmt.execute();
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    count++;
                }
            }
        }
        assertEquals(count, 100);
    }
}
