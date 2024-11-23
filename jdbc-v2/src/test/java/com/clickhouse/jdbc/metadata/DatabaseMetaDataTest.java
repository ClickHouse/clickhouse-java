package com.clickhouse.jdbc.metadata;

import com.clickhouse.jdbc.JdbcIntegrationTest;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test
    public void testGetColumns() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            final String tableName = "test_get_columns_1";
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);

            StringBuilder createTableStmt = new StringBuilder("CREATE TABLE " + tableName + " (");
            List<String> columnNames = Arrays.asList("id", "name", "float1", "fixed_string1", "decimal_1", "nullable_column");
            List<String> columnTypes = Arrays.asList("UInt64", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))");
            List<Integer> columnSizes = Arrays.asList(8, 0, 4, 10, 10, 5);
            List<Integer> columnJDBCDataTypes = Arrays.asList(Types.BIGINT, Types.VARCHAR, Types.FLOAT, Types.CHAR, Types.DECIMAL, Types.DECIMAL);

            for (int i = 0; i < columnNames.size(); i++) {
                createTableStmt.append(columnNames.get(i)).append(" ").append(columnTypes.get(i)).append(',');
            }
            createTableStmt.setLength(createTableStmt.length() - 1);
            createTableStmt.append(") ENGINE = MergeTree ORDER BY ()");
            conn.createStatement().execute(createTableStmt.toString());

             DatabaseMetaData dbmd = conn.getMetaData();
             ResultSet rs = dbmd.getColumns("default", null, tableName, null);

             int count = 0;
             while (rs.next()) {
                 String columnName = rs.getString("COLUMN_NAME");
                 assertTrue(columnNames.contains(columnName));

                 assertEquals(rs.getString("TABLE_CAT"), "default");
                 assertEquals(rs.getString("TABLE_SCHEM"), "");
                 assertEquals(rs.getString("TABLE_NAME"), tableName);

                 System.out.println(rs.getString("TYPE_NAME") + " DATA_TYPE: " + rs.getString("DATA_TYPE") + " decimal " + rs.getInt("DECIMAL_DIGITS"));

                 assertEquals(rs.getString("TYPE_NAME"), columnTypes.get(columnNames.indexOf(columnName)));
                 assertEquals(rs.getInt("DATA_TYPE"), columnJDBCDataTypes.get(columnNames.indexOf(columnName)));


                 assertEquals(rs.getInt("COLUMN_SIZE"), columnSizes.get(columnNames.indexOf(columnName)) );
                 assertEquals(rs.getInt("ORDINAL_POSITION"), columnNames.indexOf(columnName) + 1);

//                 assertEquals(rs.getInt("DECIMAL_DIGITS"), 64);
//                 assertEquals(rs.getInt("NUM_PREC_RADIX"), 2);
//                 assertEquals(rs.getInt("NULLABLE"), DatabaseMetaData.attributeNullableUnknown);
//                 assertEquals(rs.getString("IS_NULLABLE"), "");
                count++;
             }
             Assert.assertEquals(count, columnNames.size());
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

    @Test
    public void testGetServerVersions() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            Assert.assertTrue(dbmd.getDatabaseMajorVersion() >= 23); // major version is year and cannot be less than LTS version we test with
            Assert.assertTrue(dbmd.getDatabaseMinorVersion() > 0); // minor version is always greater than 0
            Assert.assertFalse(dbmd.getDatabaseProductVersion().isEmpty(), "Version cannot be blank string");
            Assert.assertEquals(dbmd.getUserName(), "default");
        }
    }

    @Test
    public void testGetTypeInfo() throws Exception {
        Assert.fail("Not implemented");
    }

    @Test
    public void testGetProcedures() throws Exception {
        Assert.fail("Not implemented");
    }

    @Test
    public void testGetClientInfoProperties() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getClientInfoProperties()) {
                Assert.assertTrue(rs.next());
            }
        }
        Assert.fail("Not implemented");
    }

    @Test
    public void testGetCatalogTerm() throws Exception {
        // TODO: test support for catalog and schema terms
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            Assert.assertEquals(dbmd.getCatalogTerm(), "catalog");
        }
    }
}
