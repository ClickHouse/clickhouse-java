package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.clickhouse.jdbc.ClientInfoProperties;
import com.clickhouse.jdbc.DriverProperties;
import com.clickhouse.jdbc.JdbcIntegrationTest;
import com.clickhouse.jdbc.internal.JdbcUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


@Test(groups = { "integration" })
public class DatabaseMetaDataTest extends JdbcIntegrationTest {
    @Test(groups = { "integration" })
    public void testGetColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            final String tableName = "get_columns_metadata_test";
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("" +
                        "CREATE TABLE " + tableName + " (id Int32, name String NOT NULL, v1 Nullable(Int8), v2 Array(Int8)) " +
                        "ENGINE MergeTree ORDER BY ()");
            }

            DatabaseMetaData dbmd = conn.getMetaData();


            try (ResultSet rs = dbmd.getColumns(null, getDatabase(), tableName.substring(0, tableName.length() - 3) + "%", null)) {

                List<String> expectedColumnNames = Arrays.asList(
                        "TABLE_CAT",
                        "TABLE_SCHEM",
                        "TABLE_NAME",
                        "COLUMN_NAME",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "COLUMN_SIZE",
                        "BUFFER_LENGTH",
                        "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "COLUMN_DEF",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SCOPE_CATALOG",
                        "SCOPE_SCHEMA",
                        "SCOPE_TABLE",
                        "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT",
                        "IS_GENERATEDCOLUMN"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.VARCHAR,
                        Types.VARCHAR
                );

                assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "id");
                assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getObject("DATA_TYPE"), Types.INTEGER);
                assertEquals(rs.getString("TYPE_NAME"), "Int32");
                assertFalse(rs.getBoolean("NULLABLE"));

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "name");
                assertEquals(rs.getInt("DATA_TYPE"), Types.VARCHAR);
                assertEquals(rs.getObject("DATA_TYPE"), Types.VARCHAR);
                assertEquals(rs.getString("TYPE_NAME"), "String");
                assertFalse(rs.getBoolean("NULLABLE"));

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "v1");
                assertEquals(rs.getInt("DATA_TYPE"), Types.TINYINT);
                assertEquals(rs.getObject("DATA_TYPE"), Types.TINYINT);
                assertEquals(rs.getString("TYPE_NAME"), "Nullable(Int8)");
                assertTrue(rs.getBoolean("NULLABLE"));

                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_SCHEM"), getDatabase());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("COLUMN_NAME"), "v2");
                assertEquals(rs.getInt("DATA_TYPE"), Types.ARRAY);
                assertEquals(rs.getObject("DATA_TYPE"), Types.ARRAY);
                assertEquals(rs.getString("TYPE_NAME"), "Array(Int8)");
                assertFalse(rs.getBoolean("NULLABLE"));
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetColumnsWithTable() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            final String tableName = "test_get_columns_1";
            conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);

            StringBuilder createTableStmt = new StringBuilder("CREATE TABLE " + tableName + " (");
            List<String> columnNames = Arrays.asList("id", "huge_integer", "name", "float1", "fixed_string1", "decimal_1", "nullable_column", "date", "datetime");
            List<String> columnTypes = Arrays.asList("Int64", "UInt128", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))", "Date", "DateTime");
            List<Integer> columnSizes = Arrays.asList(8, 16, 0, 4, 10, 10, 5, 2, 0);
            List<Integer> columnJDBCDataTypes = Arrays.asList(Types.BIGINT, Types.OTHER, Types.VARCHAR, Types.FLOAT, Types.VARCHAR, Types.DECIMAL, Types.DECIMAL, Types.DATE, Types.TIMESTAMP);
            List<String> columnTypeNames = Arrays.asList("Int64", "UInt128", "String", "Float32", "FixedString(10)", "Decimal(10, 2)", "Nullable(Decimal(5, 4))", "Date", "DateTime");
            List<Boolean> columnNullable = Arrays.asList(false, false, false, false, false, false, true, false, false);
            List<Integer> columnDecimalDigits = Arrays.asList(null, null, null, null, null, 2, 4, null, null);
            List<Integer> columnRadix = Arrays.asList(2, 2, null, null, null, 10, 10, null, null);

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
                    assertEquals(rs.getInt("DECIMAL_DIGITS"), 0); // should not throw exception
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
            List<String> expectedColumnNames = Arrays.asList(
                    "TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    "REMARKS",
                    "TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "SELF_REFERENCING_COL_NAME",
                    "REF_GENERATION");

            List<Integer> expectedTableTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR);

            ResultSet rs = dbmd.getTables("system", null, "numbers", null);
            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedTableTypes);
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
            ResultSetMetaDataImplTest.assertColumnNames(rs, "TABLE_CAT");
        }
    }

    @Test(groups = { "integration" })
    public void testGetTableTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTableTypes();
            List<String> sortedTypes = Arrays.asList(DatabaseMetaDataImpl.TABLE_TYPES);
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
                List<String> expectedColumnNames = Arrays.asList(
                        "TYPE_NAME",
                        "DATA_TYPE",
                        "PRECISION",
                        "LITERAL_PREFIX",
                        "LITERAL_SUFFIX",
                        "CREATE_PARAMS",
                        "NULLABLE",
                        "CASE_SENSITIVE",
                        "SEARCHABLE",
                        "UNSIGNED_ATTRIBUTE",
                        "FIXED_PREC_SCALE",
                        "AUTO_INCREMENT",
                        "LOCAL_TYPE_NAME",
                        "MINIMUM_SCALE",
                        "MAXIMUM_SCALE",
                        "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB",
                        "NUM_PREC_RADIX"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.BOOLEAN,
                        Types.SMALLINT,
                        Types.BOOLEAN,
                        Types.BOOLEAN,
                        Types.BOOLEAN,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.SMALLINT,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.INTEGER
                );


                // check type match with
                int count = 0;
                ResultSetMetaData rsMetaData = rs.getMetaData();
                assertProcedureColumns(rsMetaData, expectedColumnNames, expectedColumnTypes);

                while (rs.next()) {
                    count++;
                    ClickHouseDataType dataType;
                    try {
                        dataType = ClickHouseDataType.of( rs.getString("TYPE_NAME"));
                    } catch (Exception e) {
                        continue; // skip. we have another test and will catch it anyway.
                    }
                    assertEquals(ClickHouseDataType.of(rs.getString(1)), dataType);
                    assertEquals(rs.getInt("DATA_TYPE"),
                            (int) JdbcUtils.convertToSqlType(dataType).getVendorTypeNumber(),
                            "Type mismatch for " + dataType.name() + ": expected " +
                                    JdbcUtils.convertToSqlType(dataType).getVendorTypeNumber() +
                                    " but was " + rs.getInt("DATA_TYPE") + " for TYPE_NAME: " + rs.getString("TYPE_NAME"));

                    assertEquals(rs.getInt("DATA_TYPE"), rs.getObject("DATA_TYPE"));

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
                    assertFalse(rs.getBoolean("FIXED_PREC_SCALE"));
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

    @Test(groups = {"integration"})
    public void testFindNestedTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTypeInfo()) {
                Set<String> nestedTypes = Arrays.stream(ClickHouseDataType.values())
                        .filter(dt -> dt.isNested()).map(dt -> dt.name()).collect(Collectors.toSet());

                while (rs.next()) {
                    String typeName = rs.getString("TYPE_NAME");
                    nestedTypes.remove(typeName);
                }

                assertTrue(nestedTypes.isEmpty(), "Nested types " + nestedTypes + " not found");
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

                List<String> expectedColumnNames = Arrays.asList(
                        "FUNCTION_CAT",
                        "FUNCTION_SCHEM",
                        "FUNCTION_NAME",
                        "REMARKS",
                        "FUNCTION_TYPE",
                        "SPECIFIC_NAME"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.VARCHAR
                );

                assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);

                assertTrue(rs.next());
                assertNull(rs.getString("FUNCTION_CAT"));
                assertNull(rs.getString("FUNCTION_SCHEM"));
                assertEquals(rs.getString("FUNCTION_NAME"), "mapContains");
                assertFalse(rs.getString("REMARKS").isEmpty());
                assertEquals(rs.getShort("FUNCTION_TYPE"), DatabaseMetaData.functionResultUnknown);
                assertEquals(rs.getString("SPECIFIC_NAME"), "mapContains");
            }
        }
    }

    @Test(groups = { "integration" })
    public void testGetFunctionColumns() throws Exception {
        if (ClickHouseVersion.of(getServerVersion()).check("(,23.8]")) {
            return; //  Illegal column Int8 of argument of function concat. (ILLEGAL_COLUMN)  TODO: fix in JDBC
        }

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getFunctionColumns(null, null, "mapContains", null)) {
                assertFalse(rs.next());
                List<String> expectedColumnNames = Arrays.asList(
                        "FUNCTION_CAT",
                        "FUNCTION_SCHEM",
                        "FUNCTION_NAME",
                        "COLUMN_NAME",
                        "COLUMN_TYPE",
                        "DATA_TYPE",
                        "TYPE_NAME",
                        "PRECISION",
                        "LENGTH",
                        "SCALE",
                        "RADIX",
                        "NULLABLE",
                        "REMARKS",
                        "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION",
                        "IS_NULLABLE",
                        "SPECIFIC_NAME"
                );

                List<Integer> expectedColumnTypes = Arrays.asList(
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.VARCHAR,
                        Types.SMALLINT,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.SMALLINT,
                        Types.SMALLINT,
                        Types.SMALLINT,
                        Types.VARCHAR,
                        Types.INTEGER,
                        Types.INTEGER,
                        Types.VARCHAR,
                        Types.VARCHAR
                );

                assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);

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


    @Test(groups = {"integration"})
    public void testGetIndexInfoColumnType() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "NON_UNIQUE",
                    "INDEX_QUALIFIER",
                    "INDEX_NAME",
                    "TYPE",
                    "ORDINAL_POSITION",
                    "COLUMN_NAME",
                    "ASC_OR_DESC",
                    "CARDINALITY",
                    "PAGES",
                    "FILTER_CONDITION");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.BOOLEAN,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.BIGINT,
                    Types.BIGINT,
                    Types.VARCHAR
            );

            ResultSet rs = dbmd.getIndexInfo(null, null, null, false, false);
            assertFalse(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertProcedureColumns(rsmd, expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetProcedures() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            List<String> columnNames = Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED1",
                    "RESERVED2", "RESERVED3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME");
            List<Integer> columnTypes = Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                    Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR);

            ResultSet rs = dbmd.getProcedures(null, null, null);
            assertFalse(rs.next());
            ResultSetMetaData rsmd = rs.getMetaData();
            assertProcedureColumns(rsmd, columnNames, columnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetProceduresColumnType() throws Exception {

        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();

            List<String> expectedColumnNames = Arrays.asList("PROCEDURE_CAT",
                    "PROCEDURE_SCHEM",
                    "PROCEDURE_NAME",
                    "COLUMN_NAME",
                    "COLUMN_TYPE",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "PRECISION",
                    "LENGTH",
                    "SCALE",
                    "RADIX",
                    "NULLABLE",
                    "REMARKS",
                    "COLUMN_DEF",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION",
                    "IS_NULLABLE",
                    "SPECIFIC_NAME");
            List<Integer> columnTypes = Arrays.asList(Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR);
            ResultSetMetaData rsmd = dbmd.getProcedureColumns(null, null, null, null).getMetaData();

            assertProcedureColumns(rsmd, expectedColumnNames, columnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetColumnPrivileges() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumnPrivileges(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "GRANTOR",
                    "GRANTEE",
                    "PRIVILEGE",
                    "IS_GRANTABLE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetTablePrivileges() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTablePrivileges(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "GRANTOR",
                    "GRANTEE",
                    "PRIVILEGE",
                    "IS_GRANTABLE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetVersionColumnsColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getVersionColumns(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("SCOPE",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "COLUMN_SIZE",
                    "BUFFER_LENGTH",
                    "DECIMAL_DIGITS",
                    "PSEUDO_COLUMN");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.SMALLINT,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetPrimaryKeysColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getPrimaryKeys(null, null, null);

            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "KEY_SEQ",
                    "PK_NAME");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetImportedKeys() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getImportedKeys(null, null, null);
            assertFalse(rs.next());

            List<String> expectedColumnNames = Arrays.asList(
                    "PKTABLE_CAT",
                    "PKTABLE_SCHEM",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CAT",
                    "FKTABLE_SCHEM",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "KEY_SEQ",
                    "UPDATE_RULE",
                    "DELETE_RULE",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetExportedKeys() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getExportedKeys(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("PKTABLE_CAT",
                    "PKTABLE_SCHEM",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CAT",
                    "FKTABLE_SCHEM",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "KEY_SEQ",
                    "UPDATE_RULE",
                    "DELETE_RULE",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetCrossReference() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getCrossReference(null, null, null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("PKTABLE_CAT",
                    "PKTABLE_SCHEM",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CAT",
                    "FKTABLE_SCHEM",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "KEY_SEQ",
                    "UPDATE_RULE",
                    "DELETE_RULE",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetUDTs() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getUDTs(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "CLASS_NAME",
                    "DATA_TYPE",
                    "REMARKS",
                    "BASE_TYPE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetSuperTypes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSuperTypes(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "SUPERTYPE_CAT",
                    "SUPERTYPE_SCHEM",
                    "SUPERTYPE_NAME");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    @Test(groups = {"integration"})
    public void testGetSuperTables() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getSuperTables(null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "SUPERTABLE_NAME");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetBestRowIdentifier() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getBestRowIdentifier(null, null, null, 0, true);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("SCOPE",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "COLUMN_SIZE",
                    "BUFFER_LENGTH",
                    "DECIMAL_DIGITS",
                    "PSEUDO_COLUMN");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.SMALLINT,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.SMALLINT,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetAttributes() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getAttributes(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TYPE_CAT",
                    "TYPE_SCHEM",
                    "TYPE_NAME",
                    "ATTR_NAME",
                    "DATA_TYPE",
                    "ATTR_TYPE_NAME",
                    "ATTR_SIZE",
                    "DECIMAL_DIGITS",
                    "NUM_PREC_RADIX",
                    "NULLABLE",
                    "REMARKS",
                    "ATTR_DEF",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION",
                    "IS_NULLABLE",
                    "SCOPE_CATALOG",
                    "SCOPE_SCHEMA",
                    "SCOPE_TABLE",
                    "SOURCE_DATA_TYPE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.SMALLINT
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }


    @Test(groups = {"integration"})
    public void testGetPseudoColumns() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getPseudoColumns(null, null, null, null);
            assertFalse(rs.next());
            List<String> expectedColumnNames = Arrays.asList("TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "COLUMN_SIZE",
                    "DECIMAL_DIGITS",
                    "NUM_PREC_RADIX",
                    "COLUMN_USAGE",
                    "REMARKS",
                    "CHAR_OCTET_LENGTH",
                    "IS_NULLABLE");

            List<Integer> expectedColumnTypes = Arrays.asList(
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.INTEGER,
                    Types.VARCHAR,
                    Types.VARCHAR,
                    Types.INTEGER,
                    Types.VARCHAR
            );

            assertProcedureColumns(rs.getMetaData(), expectedColumnNames, expectedColumnTypes);
        }
    }

    private void assertProcedureColumns(ResultSetMetaData rsmd, List<String> expectedColumnNames, List<Integer> expectedColumnTypes) throws SQLException {
        int columnCount = rsmd.getColumnCount();
        assertEquals(columnCount, expectedColumnNames.size(), "number of columns");
        for (int i = 1; i <= columnCount; i++) {
            String columnName = rsmd.getColumnName(i);
            int columnType = rsmd.getColumnType(i);
            assertEquals(columnName, expectedColumnNames.get(i - 1), "Column name mismatch");
            assertEquals(columnType, expectedColumnTypes.get(i - 1), "Column type mismatch for column name " + columnName + " (" + i + ")");
        }
    }



    @Test(groups = {"integration"})
    public void testGetDatabaseMajorVersion() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int majorVersion = dbmd.getDatabaseMajorVersion();
            String version =  getServerVersion();
            int majorVersionOfServer = Integer.parseInt(version.split("\\.")[0]);
            assertEquals(majorVersion, majorVersionOfServer, "Major version");
        }
    }


    @Test(groups = {"integration"})
    public void testGetDatabaseMinorVersion() throws Exception {
        try (Connection conn = getJdbcConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int minorVersion = dbmd.getDatabaseMinorVersion();
            String version =  getServerVersion();
            int minorVersionOfServer = Integer.parseInt(version.split("\\.")[1]);
            assertEquals(minorVersion, minorVersionOfServer, "Minor version");
        }
    }
}
