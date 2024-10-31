package com.clickhouse.jdbc.metadata;

import com.clickhouse.jdbc.JdbcIntegrationTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import static org.testng.Assert.assertEquals;

public class ResultSetMetaDataTest extends JdbcIntegrationTest {
    @Test
    public void testGetColumnCount() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS b, 3 AS c");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(3, rsmd.getColumnCount());
            }
        }
    }

    @Test
    public void testGetColumnLabel() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals("a", rsmd.getColumnLabel(1));
            }
        }
    }

    @Test
    public void testGetColumnName() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals("a", rsmd.getColumnName(1));
            }
        }
    }

    @Test
    public void testGetColumnTypeIntegers() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toInt8(1), toInt16(1), toInt32(1), toInt64(1) AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.TINYINT);
                assertEquals(rsmd.getColumnType(2), Types.SMALLINT);
                assertEquals(rsmd.getColumnType(3), Types.INTEGER);
                assertEquals(rsmd.getColumnType(4), Types.BIGINT);
            }
        }
    }

    @Test
    public void testGetColumnTypeFloats() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toFloat32(1), toFloat64(1) AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.FLOAT);
                assertEquals(rsmd.getColumnType(2), Types.DOUBLE);
            }
        }
    }

    @Test
    public void testGetColumnTypeString() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toString(1) AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.CHAR);
            }
        }
    }

    @Test
    public void testGetColumnTypeDateAndTime() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toDate('2021-01-01') AS a, toDateTime('2021-01-01 00:00:00') AS b");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.DATE);
                assertEquals(rsmd.getColumnType(2), Types.TIMESTAMP);
            }
        }
    }

    @Test
    public void testGetColumnTypeName() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toInt16(1), toUInt16(1) AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnTypeName(1), "Int16");
                assertEquals(rsmd.getColumnTypeName(2), "UInt16");
            }
        }
    }

    @Test
    public void testGetColumnDisplaySize() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnDisplaySize(1), 80);
            }
        }
    }

    @Test
    public void testGetColumnPrecision() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getPrecision(1), 3);
            }
        }
    }

    @Test
    public void testGetColumnScale() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toInt8(1), toDecimal32(1, 5), toDecimal64(1, 5)  AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getScale(1), 0);
                assertEquals(rsmd.getScale(2), 5);
                assertEquals(rsmd.getScale(3), 5);
            }
        }
    }
}
