package com.clickhouse.jdbc.metadata;

import com.clickhouse.jdbc.JdbcIntegrationTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = { "integration" })
public class ResultSetMetaDataImplTest extends JdbcIntegrationTest {

    @Test(groups = { "integration" })
    public void testConstants() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS b, 3 AS c");
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    assertFalse(rsmd.isAutoIncrement(i));
                    assertFalse(rsmd.isWritable(i));
                    assertTrue(rsmd.isReadOnly(i));
                    assertFalse(rsmd.isDefinitelyWritable(i));
                    assertFalse(rsmd.isCurrency(i));
                    assertTrue(rsmd.isSearchable(i));
                    assertEquals(rsmd.getTableName(i), "");
                    assertEquals(rsmd.getCatalogName(i), "");
                    assertEquals(rsmd.getSchemaName(i),  getDatabase());
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testColumnIndexOutOfBoundCheck() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS b, 3 AS c");
                ResultSetMetaData rsmd = rs.getMetaData();
                int lastColumnIndex = rsmd.getColumnCount() + 1;
                for (int i = 0; i <= lastColumnIndex; i += lastColumnIndex ) {
                    final int outOfBoundIndex = i;
                    Assert.assertThrows(SQLException.class, () -> rsmd.getColumnClassName(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getColumnType(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getColumnTypeName(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getColumnLabel(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getColumnName(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getColumnDisplaySize(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getScale(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getPrecision(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isCaseSensitive(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isSigned(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isSearchable(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isCurrency(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isNullable(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isReadOnly(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isWritable(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isDefinitelyWritable(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.getTableName(outOfBoundIndex));
                    Assert.assertThrows(SQLException.class, () -> rsmd.isAutoIncrement(outOfBoundIndex));
                }
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnCount() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS b, 3 AS c");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnCount(), 3);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnLabel() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnLabel(1), "a");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnName() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnName(1), "a");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnTypeIntegers() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toInt8(1), toInt16(1), toInt32(1), toInt64(1) AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.TINYINT);
                assertEquals(rsmd.getColumnClassName(1), Byte.class.getName());
                assertEquals(rsmd.getColumnType(2), Types.SMALLINT);
                assertEquals(rsmd.getColumnClassName(2), Short.class.getName());
                assertEquals(rsmd.getColumnType(3), Types.INTEGER);
                assertEquals(rsmd.getColumnClassName(3), Integer.class.getName());
                assertEquals(rsmd.getColumnType(4), Types.BIGINT);
                assertEquals(rsmd.getColumnClassName(4), Long.class.getName());

                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    assertTrue(rsmd.isCaseSensitive(i));
                    assertFalse(rsmd.isCurrency(i));
                    assertEquals(rsmd.isNullable(i), ResultSetMetaData.columnNoNulls);
                    assertTrue(rsmd.isSearchable(i));
                    assertTrue(rsmd.isSigned(i));
                }
            }
        }
    }

    @Test
    public void testGetColumnTypeMap() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("select map('a', 1) as a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.OTHER);
                assertEquals(rsmd.getColumnClassName(1), Object.class.getName());
            }
        }
    }

    @Test(groups = { "integration" })
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

    @Test(groups = { "integration" })
    public void testGetColumnTypeString() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT toString(1) AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnType(1), Types.VARCHAR);
            }
        }
    }

    @Test(groups = { "integration" })
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

    @Test(groups = { "integration" })
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

    @Test(groups = { "integration" })
    public void testGetColumnDisplaySize() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getColumnDisplaySize(1), 80);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnPrecision() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1 AS a");
                ResultSetMetaData rsmd = rs.getMetaData();
                assertEquals(rsmd.getPrecision(1), 3);
            }
        }
    }

    @Test(groups = { "integration" })
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

    static void assertColumnNames(ResultSet rs, String... names) throws Exception {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(names.length, metadata.getColumnCount());
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            assertEquals(names[i], metadata.getColumnName(i + 1));
        }
    }
    static void assertColumnTypes(ResultSet rs, String... types) throws Exception {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(types.length, metadata.getColumnCount());
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            assertEquals(types[i], metadata.getColumnTypeName(i + 1));
        }
    }
}
