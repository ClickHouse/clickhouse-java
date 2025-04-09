package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.command.CommandResponse;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.JdbcIntegrationTest;
import com.clickhouse.jdbc.internal.ClientInfoProperties;
import com.clickhouse.jdbc.internal.DriverProperties;
import com.clickhouse.jdbc.internal.JdbcUtils;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.*;


public class DatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test(groups = { "integration" })
    public void testGetColumns() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            final String tableName = "test_get_columns_1";
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);

            StringBuilder createTableStmt = new StringBuilder("CREATE TABLE " + tableName + " (");
            List<String> columnNames = Arrays.asList("id", "name", "float1", "fixed_string1", "decimal_1", "nullable_column", "date", "datetime");
            List<String> columnTypes = Arrays.asList("UInt64", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))", "Date", "DateTime");
            List<Integer> columnSizes = Arrays.asList(8, 0, 4, 10, 10, 5, 2, 0);
            List<Integer> columnJDBCDataTypes = Arrays.asList(Types.BIGINT, Types.VARCHAR, Types.FLOAT, Types.VARCHAR, Types.DECIMAL, Types.DECIMAL, Types.DATE, Types.TIMESTAMP);
            List<String> columnTypeNames = Arrays.asList("UInt64", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))", "Date", "DateTime");
            List<Boolean> columnNullable = Arrays.asList(false, false, false, false, false, true, false, false);
            List<Integer> columnDecimalDigits = Arrays.asList(null, null, null, null, 2, 4, null, null);
            List<Integer> columnRadix = Arrays.asList(2, null, null, null, 10, 10, null, null);

            for (int i = 0; i < columnNames.size(); i++) {
                createTableStmt.append(columnNames.get(i)).append(" ").append(columnTypes.get(i)).append(',');
            }
            createTableStmt.setLength(createTableStmt.length() - 1);
            createTableStmt.append(") ENGINE = MergeTree ORDER BY ()");
            conn.createStatement().execute(createTableStmt.toString());

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns(null, ClickHouseServerForTest.getDatabase(), tableName, null);

            int count = 0;
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int colIndex = columnNames.indexOf(columnName);
                System.out.println("Column name: " + columnName + " colIndex: " + colIndex);
                assertTrue(columnNames.contains(columnName));
                assertEquals(rs.getString("TABLE_CAT"), "");
                assertEquals(rs.getString("TABLE_SCHEM"), ClickHouseServerForTest.getDatabase());
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
            Assert.assertEquals(count, columnNames.size(), "result set is empty");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTables() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables("system", null, "numbers", null);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "numbers");
            assertEquals(rs.getString("TABLE_TYPE"), "SYSTEM TABLE");
            assertFalse(rs.next());
            rs.close();

            rs = dbmd.getTables("system", null, "numbers", new String[] { "SYSTEM TABLE" });
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), "numbers");
            assertEquals(rs.getString("TABLE_TYPE"), "SYSTEM TABLE");
            assertFalse(rs.next());
            rs.close();

            rs = dbmd.getTables("system", null, "numbers", new String[] { "TABLE" });
            assertFalse(rs.next());
            rs.close();
        }
    }


    @Test(groups = { "integration" })
    public void testGetPrimaryKeys() throws Exception {
        runQuery("SELECT 1;");
        runQuery("SYSTEM FLUSH LOGS");

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

    @Test
    public void testSchemaTerm() throws Exception {

        try (Connection connection = getJdbcConnection()){
            Assert.assertEquals(connection.getMetaData().getSchemaTerm(), "schema");
        }

        Properties prop = new Properties();
        prop.put(DriverProperties.SCHEMA_TERM.getKey(), "database");
        try (Connection connection = getJdbcConnection(prop)){
            Assert.assertEquals(connection.getMetaData().getSchemaTerm(), "database");
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
            List<String> sortedTypes = Arrays.asList(com.clickhouse.jdbc.metadata.DatabaseMetaData.TABLE_TYPES);
            Collections.sort(sortedTypes);
            for (String type: sortedTypes) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_TYPE"), type);
            }

            assertFalse(rs.next());
        }
    }

    @Test(groups = { "integration" }, enabled = false)
    public void testGetColumnsWithEmptyCatalog() throws Exception {
        // test not relevant until catalogs are implemented
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
                ResultSetMetaData rsMetaData = rs.getMetaData();
                assertTrue(rsMetaData.getColumnCount() >= 18, "Expected at least 18 columns in getTypeInfo result set");
                assertEquals(rsMetaData.getColumnType(2), Types.INTEGER);
                assertEquals(rsMetaData.getColumnType(7), Types.INTEGER);
                while (rs.next()) {
                    count++;
                    ClickHouseDataType dataType = ClickHouseDataType.of( rs.getString("TYPE_NAME"));
                    System.out.println("> " + dataType);
                    assertEquals(ClickHouseDataType.of(rs.getString(1)), dataType);
                    assertEquals(rs.getInt("DATA_TYPE"),
                            (int) JdbcUtils.convertToSqlType(dataType).getVendorTypeNumber(),
                            "Type mismatch for " + dataType.name() + ": expected " +
                                    JdbcUtils.convertToSqlType(dataType).getVendorTypeNumber() +
                                    " but was " + rs.getInt("DATA_TYPE") + " for TYPE_NAME: " + rs.getString("TYPE_NAME"));

                    assertEquals(rs.getInt("PRECISION"), dataType.getMaxPrecision());
                    assertNull(rs.getString("LITERAL_PREFIX"));
                    assertNull(rs.getString("LITERAL_SUFFIX"));
                    assertEquals(rs.getInt("MINIMUM_SCALE"), dataType.getMinScale());
                    assertEquals(rs.getInt("MAXIMUM_SCALE"), dataType.getMaxScale());
                    assertNull(rs.getString("CREATE_PARAMS"));

                    if (dataType == ClickHouseDataType.Nullable || dataType == ClickHouseDataType.Dynamic) {
                        assertEquals( rs.getShort("NULLABLE"), DatabaseMetaData.typeNullable);
                    } else {
                        assertEquals(rs.getShort("NULLABLE"), DatabaseMetaData.typeNoNulls);
                    }

                    if (dataType != ClickHouseDataType.Enum) {
                        assertEquals(rs.getBoolean("CASE_SENSITIVE"), dataType.isCaseSensitive());
                    }
                    assertEquals(rs.getInt("SEARCHABLE"), DatabaseMetaData.typeSearchable);
                    assertEquals(rs.getBoolean("UNSIGNED_ATTRIBUTE"), !dataType.isSigned());
                    assertEquals(rs.getBoolean("FIXED_PREC_SCALE"), false);
                    assertFalse(rs.getBoolean("AUTO_INCREMENT"));
                    assertEquals(rs.getString("LOCAL_TYPE_NAME"), dataType.name());
                    assertEquals(rs.getInt("MINIMUM_SCALE"), dataType.getMinScale());
                    assertEquals(rs.getInt("MAXIMUM_SCALE"), dataType.getMaxScale());
                    assertEquals(rs.getInt("SQL_DATA_TYPE"), 0);
                    assertEquals(rs.getInt("SQL_DATETIME_SUB"), 0);
                }

                assertTrue(count > 10, "At least 10 types should be returned but was " + count);
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFunctions() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,23.8]")) {
            return; //  Illegal column Int8 of argument of function concat. (ILLEGAL_COLUMN)  TODO: fix in JDBC
        }

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

    @Test(groups = { "integration" })
    public void testGetDriverVersion() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String version = dbmd.getDriverVersion();
            assertNotEquals(version, "unknown");
        }
    }
}
