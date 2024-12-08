package com.clickhouse.jdbc.metadata;

import com.clickhouse.jdbc.JdbcIntegrationTest;
import com.clickhouse.jdbc.internal.ClientInfoProperties;
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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;



public class DatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test(groups = { "integration" })
    public void testGetColumns() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            final String tableName = "test_get_columns_1";
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);

            StringBuilder createTableStmt = new StringBuilder("CREATE TABLE " + tableName + " (");
            List<String> columnNames = Arrays.asList("id", "name", "float1", "fixed_string1", "decimal_1", "nullable_column");
            List<String> columnTypes = Arrays.asList("UInt64", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))");
            List<Integer> columnSizes = Arrays.asList(8, 0, 4, 10, 10, 5);
            List<Integer> columnJDBCDataTypes = Arrays.asList(Types.BIGINT, Types.VARCHAR, Types.FLOAT, Types.CHAR, Types.DECIMAL, Types.DECIMAL);
            List<String> columnTypeNames = Arrays.asList("UInt64", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Decimal(5, 4)");
            List<Boolean> columnNullable = Arrays.asList(false, false, false, false, false, true);
            List<Integer> columnDecimalDigits = Arrays.asList(null, null, null, null, 2, 4);
            List<Integer> columnRadix = Arrays.asList(2, null, null, null, 10, 10);

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
                int colIndex = columnNames.indexOf(columnName);
                System.out.println("Column name: " + columnName + " colIndex: " + colIndex);
                assertTrue(columnNames.contains(columnName));
                assertEquals(rs.getString("TABLE_CAT"), "");
                assertEquals(rs.getString("TABLE_SCHEM"), "default");
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("TYPE_NAME"), columnTypeNames.get(colIndex));
                assertEquals(rs.getInt("DATA_TYPE"), columnJDBCDataTypes.get(colIndex));
                assertEquals(rs.getInt("COLUMN_SIZE"), columnSizes.get(colIndex));
                assertEquals(rs.getInt("ORDINAL_POSITION"), colIndex + 1);
                assertEquals(rs.getInt("NULLABLE"), columnNullable.get(colIndex) ? DatabaseMetaData.attributeNullable : DatabaseMetaData.attributeNoNulls);
                assertEquals(rs.getString("IS_NULLABLE"), columnNullable.get(colIndex) ? "YES" : "NO");

                Integer decimalDigits = columnDecimalDigits.get(colIndex);
                if (decimalDigits != null) {
                    assertEquals(rs.getInt("DECIMAL_DIGITS"), decimalDigits.intValue());
                } else {
                    assertEquals(0, rs.getInt("DECIMAL_DIGITS")); // should not throw exception
                    assertTrue(rs.wasNull());
                }
                Integer precisionRadix = columnRadix.get(colIndex);
                if (precisionRadix != null) {
                    assertEquals(rs.getInt("NUM_PREC_RADIX"), precisionRadix.intValue());
                } else {
                    rs.getInt("NUM_PREC_RADIX"); // should not throw exception
                    assertTrue(rs.wasNull());
                }
                count++;
            }
            Assert.assertEquals(count, columnNames.size());
        }
    }

    @Test(groups = { "integration" })
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


    @Test(groups = { "integration" })
    public void testGetPrimaryKeys() throws Exception {
        runQuery("SELECT 1;");
        Thread.sleep(5 * 1000); // wait for query log to be updated

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys(null, "system", "query_log");
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "query_log");
            assertEquals(rs.getString("COLUMN_NAME"), "event_date");
            assertEquals(rs.getShort("KEY_SEQ"), 1);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "query_log");
            assertEquals(rs.getString("COLUMN_NAME"), "event_time");
            assertEquals(rs.getShort("KEY_SEQ"), 2);
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testGetSchemas() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSchemas();
            boolean defaultSchemaFound = false;
            while (rs.next()) {
                if (rs.getString("TABLE_SCHEM").equals("default")) {
                    defaultSchemaFound = true;
                    break;
                }
            }

            assertTrue(defaultSchemaFound);
        }
    }

    @Test(groups = { "integration" })
    public void testGetCatalogs() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getCatalogs();
            assertFalse(rs.next());
            ResultSetMetaDataTest.assertColumnNames(rs, "TABLE_CAT");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTableTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTableTypes();
            int count = 0;
            Set<String> tableTypes = new HashSet<>(Arrays.asList("MergeTree", "Log", "Memory"));
            while (rs.next()) {
                tableTypes.remove(rs.getString("TABLE_TYPE"));
                count++;
            }

            assertTrue(count > 10);
            assertTrue(tableTypes.isEmpty(), "Not all table types are found: " + tableTypes);
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnsWithEmptyCatalog() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns("", null, "numbers", null);
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnsWithEmptySchema() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns("system", "", "numbers", null);
            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" })
    public void testGetServerVersions() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            Assert.assertTrue(dbmd.getDatabaseMajorVersion() >= 23); // major version is year and cannot be less than LTS version we test with
            Assert.assertTrue(dbmd.getDatabaseMinorVersion() > 0); // minor version is always greater than 0
            Assert.assertFalse(dbmd.getDatabaseProductVersion().isEmpty(), "Version cannot be blank string");
            Assert.assertEquals(dbmd.getUserName(), "default");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTypeInfo() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTypeInfo()) {
                int count = 0;
                while (rs.next()) {
                    count++;
                    Assert.assertTrue(rs.getString("TYPE_NAME").length() > 0);
                }

                assertTrue(count > 10, "At least 10 types should be returned but was " + count);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFunctions() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getFunctions(null, null, "mapContains")) {
                assertTrue(rs.next());
                assertNull(rs.getString("FUNCTION_CAT"));
                assertNull(rs.getString("FUNCTION_SCHEM"));
                assertEquals(rs.getString("FUNCTION_NAME"), "mapContains");
                assertTrue(rs.getString("REMARKS").startsWith("Checks whether the map has the specified key"));
                assertEquals(rs.getShort("FUNCTION_TYPE"), DatabaseMetaData.functionResultUnknown);
                assertEquals(rs.getString("SPECIFIC_NAME"), "mapContains");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetClientInfoProperties() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getClientInfoProperties()) {
                for (ClientInfoProperties p : ClientInfoProperties.values()) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getString("NAME"), p.getKey());
                    Assert.assertEquals(rs.getInt("MAX_LEN"), p.getMaxValue());
                    Assert.assertEquals(rs.getString("DEFAULT_VALUE"), p.getDefaultValue());
                    Assert.assertEquals(rs.getString("DESCRIPTION"), p.getDescription());
                }
            }
        }
    }
}
