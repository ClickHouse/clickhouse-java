package com.clickhouse.jdbc.metadata;

import com.clickhouse.jdbc.JdbcIntegrationTest;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.sql.DatabaseMetaData;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test
    public void testGetColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getColumns("system", null, "numbers", null);
             assertTrue(rs.next());
             assertEquals(rs.getString("TABLE_NAME"), "numbers");
             assertEquals(rs.getString("COLUMN_NAME"), "number");
             assertEquals(rs.getInt("DATA_TYPE"), Types.BIGINT);
             assertEquals(rs.getString("TYPE_NAME"), "UInt64");
             assertEquals(rs.getInt("COLUMN_SIZE"), 64);
             assertEquals(rs.getInt("DECIMAL_DIGITS"), 64);
             assertEquals(rs.getInt("NUM_PREC_RADIX"), 2);
             assertEquals(rs.getInt("NULLABLE"), DatabaseMetaData.attributeNullableUnknown);
             assertEquals(rs.getString("IS_NULLABLE"), "");
             assertEquals(rs.getInt("ORDINAL_POSITION"), 1);
             assertFalse(rs.next());
        }
    }

    @Test
    public void testGetTables() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getTables("system", null, "numbers", null);
             assertTrue(rs.next());
             assertEquals(rs.getString("TABLE_NAME"), "numbers");
             assertEquals(rs.getString("TABLE_TYPE"), "SystemNumbers");
             assertFalse(rs.next());
        }
    }

    @Ignore("ClickHouse does not support primary keys")
    @Test
    public void testGetPrimaryKeys() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getPrimaryKeys("system", null, "numbers");
             assertTrue(rs.next());
             assertEquals(rs.getString("TABLE_NAME"), "numbers");
             assertEquals(rs.getString("COLUMN_NAME"), "number");
             assertEquals(rs.getShort("KEY_SEQ"), 1);
             assertFalse(rs.next());
        }
    }

    @Test
    public void testGetSchemas() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getSchemas();
             assertTrue(rs.next());
             assertEquals(rs.getString("TABLE_SCHEM"), "INFORMATION_SCHEMA");
        }
    }

    @Test
    public void testGetCatalogs() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getCatalogs();
             assertTrue(rs.next());
             assertEquals(rs.getString("TABLE_CAT"), "INFORMATION_SCHEMA");
        }
    }

    @Test
    public void testGetTableTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getTableTypes();
             assertTrue(rs.next());
             assertEquals(rs.getString("TABLE_TYPE"), "MergeTree");
        }
    }

    @Test
    public void testGetColumnsWithEmptyCatalog() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getColumns("", null, "numbers", null);
             assertFalse(rs.next());
        }
    }

    @Test
    public void testGetColumnsWithEmptySchema() throws Exception {
        try (Connection conn = getJdbcConnection()) {
             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getColumns("system", "", "numbers", null);
             assertFalse(rs.next());
        }
    }
}
