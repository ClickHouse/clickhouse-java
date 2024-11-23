package com.clickhouse.jdbc;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class StatementTest extends JdbcIntegrationTest {
    @Test
    public void testExecuteQuerySimpleNumbers() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1 AS num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getByte(1), 1);
                    assertEquals(rs.getByte("num"), 1);
                    assertEquals(rs.getShort(1), 1);
                    assertEquals(rs.getShort("num"), 1);
                    assertEquals(rs.getInt(1), 1);
                    assertEquals(rs.getInt("num"), 1);
                    assertEquals(rs.getLong(1), 1);
                    assertEquals(rs.getLong("num"), 1);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteQuerySimpleFloats() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1.1 AS num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 1.1f);
                    assertEquals(rs.getFloat("num"), 1.1f);
                    assertEquals(rs.getDouble(1), 1.1);
                    assertEquals(rs.getDouble("num"), 1.1);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteQueryBooleans() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT true AS flag")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertTrue(rs.getBoolean("flag"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteQueryStrings() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 'Hello' AS words")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "Hello");
                    assertEquals(rs.getString("words"), "Hello");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteQueryNulls() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT NULL AS nothing")) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject(1));
                    assertNull(rs.getObject("nothing"));
                    assertNull(rs.getString(1));
                    assertNull(rs.getString("nothing"));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteQueryDates() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT toDate('2020-01-01 12:10:07') AS date, toDateTime('2020-01-01 10:11:12', 'Asia/Istanbul') AS datetime")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1).toString(), "2020-01-01");
                    assertEquals(rs.getDate("date").toString(), "2020-01-01");
                    assertEquals(rs.getDate(1).toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getDate("date").toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getDate(1, null).toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getDate("date", null).toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getString(1), "2020-01-01T00:00Z[UTC]");
                    assertEquals(rs.getString("date"), "2020-01-01T00:00Z[UTC]");
                    assertEquals(rs.getDate(2).toString(), "2020-01-01");
                    assertEquals(rs.getDate("datetime").toString(), "2020-01-01");
                    assertEquals(rs.getDate(2).toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getDate("datetime").toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getDate(2, null).toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getDate("datetime", null).toLocalDate().toString(), "2020-01-01");
                    assertEquals(rs.getString(2), "2020-01-01T10:11:12+03:00[Asia/Istanbul]");
                    assertEquals(rs.getString("datetime"), "2020-01-01T10:11:12+03:00[Asia/Istanbul]");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteUpdateSimpleNumbers() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".simpleNumbers (num UInt8) ENGINE = Memory"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".simpleNumbers VALUES (1), (2), (3)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT num FROM " + getDatabase() + ".simpleNumbers ORDER BY num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 1);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 2);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 3);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteUpdateSimpleFloats() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".simpleFloats (num Float32) ENGINE = Memory"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".simpleFloats VALUES (1.1), (2.2), (3.3)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT num FROM " + getDatabase() + ".simpleFloats ORDER BY num")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 1.1f);
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 2.2f);
                    assertTrue(rs.next());
                    assertEquals(rs.getFloat(1), 3.3f);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteUpdateBooleans() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".booleans (id UInt8, flag Boolean) ENGINE = Memory"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".booleans VALUES (0, true), (1, false), (2, true)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT flag FROM " + getDatabase() + ".booleans ORDER BY id")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertTrue(rs.next());
                    assertFalse(rs.getBoolean(1));
                    assertTrue(rs.next());
                    assertTrue(rs.getBoolean(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteUpdateStrings() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".strings (id UInt8, words String) ENGINE = Memory"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".strings VALUES (0, 'Hello'), (1, 'World'), (2, 'ClickHouse')"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT words FROM " + getDatabase() + ".strings ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "Hello");
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "World");
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "ClickHouse");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteUpdateNulls() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".nulls (id UInt8, nothing Nullable(String)) ENGINE = Memory"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".nulls VALUES (0, 'Hello'), (1, NULL), (2, 'ClickHouse')"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT nothing FROM " + getDatabase() + ".nulls ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "Hello");
                    assertTrue(rs.next());
                    assertNull(rs.getString(1));
                    assertTrue(rs.next());
                    assertEquals(rs.getString(1), "ClickHouse");
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteUpdateDates() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".dates (id UInt8, date Nullable(Date), datetime Nullable(DateTime)) ENGINE = Memory"), 0);
                assertEquals(stmt.executeUpdate("INSERT INTO " + getDatabase() + ".dates VALUES (0, '2020-01-01', '2020-01-01 10:11:12'), (1, NULL, '2020-01-01 12:10:07'), (2, '2020-01-01', NULL)"), 3);
                try (ResultSet rs = stmt.executeQuery("SELECT date, datetime FROM " + getDatabase() + ".dates ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1).toString(), "2020-01-01");
                    assertEquals(rs.getDate(2).toString(), "2020-01-01");
                    assertTrue(rs.next());
                    assertNull(rs.getDate(1));
                    assertEquals(rs.getDate(2).toString(), "2020-01-01");
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1).toString(), "2020-01-01");
                    assertNull(rs.getDate(2));
                    assertFalse(rs.next());
                }
            }
        }
    }


    @Test
    public void testExecuteUpdateBatch() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                assertEquals(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + getDatabase() + ".batch (id UInt8, num UInt8) ENGINE = Memory"), 0);
                stmt.addBatch("INSERT INTO " + getDatabase() + ".batch VALUES (0, 1)");
                stmt.addBatch("INSERT INTO " + getDatabase() + ".batch VALUES (1, 2)");
                stmt.addBatch("INSERT INTO " + getDatabase() + ".batch VALUES (2, 3), (3, 4)");
                int[] counts = stmt.executeBatch();
                assertEquals(counts.length, 3);
                assertEquals(counts[0], 1);
                assertEquals(counts[1], 1);
                assertEquals(counts[2], 2);
                try (ResultSet rs = stmt.executeQuery("SELECT num FROM " + getDatabase() + ".batch ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 1);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 2);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 3);
                    assertTrue(rs.next());
                    assertEquals(rs.getShort(1), 4);
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testJdbcEscapeSyntax() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT {d '2021-11-01'} AS D, {ts '2021-08-01 12:34:56'} AS TS, " +
                        "toInt32({fn ABS(-1)}) AS FNABS, {fn CONCAT('Hello', 'World')} AS FNCONCAT, {fn UCASE('hello')} AS FNUPPER, " +
                        "{fn LCASE('HELLO')} AS FNLOWER, {fn LTRIM('  Hello  ')} AS FNLTRIM, {fn RTRIM('  Hello  ')} AS FNRTRIM, " +
                        "toInt32({fn LENGTH('Hello')}) AS FNLENGTH, toInt32({fn LOCATE('l', 'Hello')}) AS FNLOCATE, toInt32({fn MOD(10, 3)}) AS FNMOD, " +
                        "{fn SQRT(9)} AS FNSQRT, {fn SUBSTRING('Hello', 3, 2)} AS FNSUBSTRING")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getDate(1), Date.valueOf(LocalDate.of(2021, 11, 1)));
                    //assertEquals(rs.getTimestamp(2), java.sql.Timestamp.valueOf(LocalDateTime.of(2021, 11, 1, 12, 34, 56)));
                    assertEquals(rs.getInt(3), 1);
                    assertEquals(rs.getInt("FNABS"), 1);
                    assertEquals(rs.getString(4), "HelloWorld");
                    assertEquals(rs.getString("FNCONCAT"), "HelloWorld");
                    assertEquals(rs.getString(5), "HELLO");
                    assertEquals(rs.getString("FNUPPER"), "HELLO");
                    assertEquals(rs.getString(6), "hello");
                    assertEquals(rs.getString("FNLOWER"), "hello");
                    assertEquals(rs.getString(7), "Hello  ");
                    assertEquals(rs.getString("FNLTRIM"), "Hello  ");
                    assertEquals(rs.getString(8), "  Hello");
                    assertEquals(rs.getString("FNRTRIM"), "  Hello");
                    assertEquals(rs.getInt(9), 5);
                    assertEquals(rs.getInt("FNLENGTH"), 5);
                    assertEquals(rs.getInt(10), 3);
                    assertEquals(rs.getInt("FNLOCATE"), 3);
                    assertEquals(rs.getInt(11), 1);
                    assertEquals(rs.getInt("FNMOD"), 1);
                    assertEquals(rs.getDouble(12), 3);
                    assertEquals(rs.getDouble("FNSQRT"), 3);
                    assertEquals(rs.getString(13), "ll");
                    assertEquals(rs.getString("FNSUBSTRING"), "ll");
                    assertThrows(SQLException.class, () -> rs.getString(14));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testExecuteQueryTimeout() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(1);
                assertThrows(SQLException.class, () -> {
                    try (ResultSet rs = stmt.executeQuery("SELECT sleep(2)")) {
                        assertFalse(rs.next());
                    }
                });
            }
        }
    }
}
