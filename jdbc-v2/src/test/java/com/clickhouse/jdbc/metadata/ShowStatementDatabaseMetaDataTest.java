package com.clickhouse.jdbc.metadata;

import com.clickhouse.client.ClickHouseServerForTest;
import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.jdbc.ConnectionImpl;
import com.clickhouse.jdbc.DriverProperties;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Runs the whole {@link DatabaseMetaDataTest} suite with {@link DriverProperties#METADATA_USE_SHOW_STATEMENTS}
 * enabled, so {@code getSchemas()}, {@code getTables()} and {@code getColumns()} read metadata with
 * {@code SHOW DATABASES}, {@code SHOW TABLES} and {@code DESCRIBE TABLE}. The tests of this class compare the results
 * with the system table queries and cover the flag.
 */
@Test(groups = { "integration" })
public class ShowStatementDatabaseMetaDataTest extends DatabaseMetaDataTest {

    @Override
    public Connection getJdbcConnection(Properties properties) throws SQLException {
        Properties props = properties == null ? new Properties() : (Properties) properties.clone();
        props.putIfAbsent(DriverProperties.METADATA_USE_SHOW_STATEMENTS.getKey(), String.valueOf(true));
        return super.getJdbcConnection(props);
    }

    private Connection getJdbcConnectionWithDefaultFlag() throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", "default");
        info.setProperty("password", ClickHouseServerForTest.getPassword());
        info.setProperty(ClientConfigProperties.DATABASE.getKey(), getDatabase());
        return new ConnectionImpl(getEndpointString(), info);
    }

    private interface MetadataCall {
        ResultSet call(DatabaseMetaData metaData) throws SQLException;
    }

    private static final String SHOW_STATEMENTS_TABLE = "metadata_show_statements";

    private static final String SHOW_STATEMENTS_ODD_TABLE = SHOW_STATEMENTS_TABLE + " odd.'\"\\`?name";

    private static String quoteIdentifier(String name) {
        return '`' + name.replace("\\", "\\\\").replace("`", "\\`") + '`';
    }

    @DataProvider(name = "metadataCallsWithShowStatements")
    public Object[][] metadataCallsWithShowStatements() {
        final String db = getDatabase();
        final String table = SHOW_STATEMENTS_TABLE;
        final String tables = SHOW_STATEMENTS_TABLE + "%";
        return new Object[][] {
                {(MetadataCall) md -> md.getColumns(null, db, table, null), false, true},
                {(MetadataCall) md -> md.getColumns(null, db, table, "d%"), false, true},
                {(MetadataCall) md -> md.getColumns(null, db, table, "%\\_t%"), false, true},
                {(MetadataCall) md -> md.getColumns(null, db, tables, null), false, true},
                {(MetadataCall) md -> md.getColumns(null, db + "%", "metadata\\_show\\_statements%", null), false, true},
                {(MetadataCall) md -> md.getColumns(null, db, table, ""), false, false},
                {(MetadataCall) md -> md.getColumns(null, db, "metadata\\_show\\_statements odd.'%", "odd \"col\"%"), false, true},
                {(MetadataCall) md -> md.getColumns(null, "%' OR '1'='1", "%", null), false, false},
                {(MetadataCall) md -> md.getTables(null, db, "metadata\\_show\\_statements odd.'%", null), true, true},
                {(MetadataCall) md -> md.getTables(null, db, tables, null), true, true},
                {(MetadataCall) md -> md.getTables(null, db + "%", "metadata\\_show\\_statements%", null), true, true},
                {(MetadataCall) md -> md.getTables(null, db, tables, new String[]{"VIEW", "MATERIALIZED VIEW"}), true, true},
                {(MetadataCall) md -> md.getTables(null, db, tables, new String[]{"UNKNOWN"}), true, true},
                {(MetadataCall) md -> md.getTables(null, "system", "%", new String[]{"SYSTEM TABLE"}), true, true},
                {(MetadataCall) md -> md.getTables(null, db, "", null), true, false},
                {(MetadataCall) md -> md.getSchemas(null, db), false, true},
                {(MetadataCall) md -> md.getSchemas(null, "syst%"), false, true},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "metadataCallsWithShowStatements")
    public void testShowStatementsReturnSameMetadataAsSystemTables(MetadataCall call, boolean isGetTables,
                                                                   boolean expectRows) throws Exception {
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP VIEW IF EXISTS " + SHOW_STATEMENTS_TABLE + "_mv");
            stmt.executeUpdate("DROP VIEW IF EXISTS " + SHOW_STATEMENTS_TABLE + "_view");
            stmt.executeUpdate("DROP TABLE IF EXISTS " + SHOW_STATEMENTS_TABLE);
            stmt.executeUpdate("DROP TABLE IF EXISTS " + quoteIdentifier(SHOW_STATEMENTS_ODD_TABLE));
            stmt.executeUpdate("CREATE TABLE " + SHOW_STATEMENTS_TABLE + " (" +
                    "id Int32 COMMENT 'row id', u8 UInt8, i128 Int128, u256 UInt256, b Bool, nb Nullable(Bool), " +
                    "f32 Float32, d Decimal(10, 2) DEFAULT 1.5, d0 Nullable(Decimal(5, 0)), d64 Decimal64(4), " +
                    "s String, fs FixedString(4), nfs Nullable(FixedString(3)), lfs LowCardinality(FixedString(3)), " +
                    "ls LowCardinality(Nullable(String)), e Enum8('a' = 1), dt DateTime64(3, 'UTC'), dd Date, " +
                    "arr Array(Nullable(Int8)), m Map(String, Tuple(x Int32, y String)), " +
                    "nested_tuple Tuple(a Tuple(b Int32, c Nullable(String)), e UInt8), j JSON(a.b UInt32), " +
                    "sa SimpleAggregateFunction(sum, UInt64), " +
                    "nsa SimpleAggregateFunction(anyLast, Nullable(Decimal(9, 3))), " +
                    "mat Int64 MATERIALIZED id * 2, al String ALIAS toString(id), eph Int32 EPHEMERAL" +
                    ") ENGINE = MergeTree ORDER BY id COMMENT 'table comment'");
            stmt.executeUpdate("CREATE VIEW " + SHOW_STATEMENTS_TABLE + "_view AS SELECT id, d FROM " + SHOW_STATEMENTS_TABLE);
            stmt.executeUpdate("CREATE MATERIALIZED VIEW " + SHOW_STATEMENTS_TABLE + "_mv ENGINE = MergeTree ORDER BY id " +
                    "AS SELECT id, s FROM " + SHOW_STATEMENTS_TABLE);
            stmt.executeUpdate("CREATE TABLE " + quoteIdentifier(SHOW_STATEMENTS_ODD_TABLE) + " (id Int32, " +
                    quoteIdentifier("odd \"col\"?\\") + " String) ENGINE = MergeTree ORDER BY id");
        }

        Properties systemTablesProps = new Properties();
        systemTablesProps.setProperty(DriverProperties.METADATA_USE_SHOW_STATEMENTS.getKey(), "false");
        try (Connection showConn = getJdbcConnection(); Connection systemConn = getJdbcConnection(systemTablesProps);
             ResultSet showRs = call.call(showConn.getMetaData());
             ResultSet systemRs = call.call(systemConn.getMetaData())) {
            Set<String> ignoredColumns = isGetTables
                    ? new HashSet<>(Arrays.asList("REMARKS", "TYPE_SCHEM")) : Collections.emptySet();

            assertEquals(columnsOf(showRs.getMetaData(), ignoredColumns), columnsOf(systemRs.getMetaData(), ignoredColumns));
            List<List<Object>> showRows = rowsOf(showRs, ignoredColumns);
            List<List<Object>> systemRows = rowsOf(systemRs, ignoredColumns);
            assertEquals(!systemRows.isEmpty(), expectRows);
            if (isGetTables) {
                showRows.sort(Comparator.comparing(Object::toString));
                systemRows.sort(Comparator.comparing(Object::toString));
            }
            assertEquals(showRows, systemRows);
        }
    }

    private static List<String> columnsOf(ResultSetMetaData metaData, Set<String> ignoredColumns) throws SQLException {
        List<String> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String label = metaData.getColumnLabel(i);
            columns.add(label + ":" + metaData.getColumnType(i) + (ignoredColumns.contains(label) ? ""
                    : ":" + metaData.getColumnTypeName(i) + ":" + metaData.isNullable(i)));
        }
        return columns;
    }

    private static List<List<Object>> rowsOf(ResultSet rs, Set<String> ignoredColumns) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (!ignoredColumns.contains(metaData.getColumnLabel(i))) {
                    row.add(rs.getObject(i));
                }
            }
            rows.add(row);
        }
        return rows;
    }

    @DataProvider(name = "showStatementsFlagValues")
    public Object[][] showStatementsFlagValues() {
        return new Object[][] {
                {null, null},
                {"true", null},
                {"false", "flag table comment"},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "showStatementsFlagValues")
    public void testTableRemarksDependOnShowStatementsFlag(String flag, String expectedRemarks) throws Exception {
        final String tableName = "metadata_show_statements_flag";
        Properties props = new Properties();
        if (flag != null) {
            props.setProperty(DriverProperties.METADATA_USE_SHOW_STATEMENTS.getKey(), flag);
        }
        try (Connection conn = flag == null ? getJdbcConnectionWithDefaultFlag() : getJdbcConnection(props)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
                stmt.executeUpdate("CREATE TABLE " + tableName + " (id Int32) ENGINE = MergeTree ORDER BY id " +
                        "COMMENT 'flag table comment'");
            }
            try (ResultSet rs = conn.getMetaData().getTables(null, getDatabase(), tableName, null)) {
                assertTrue(rs.next());
                assertEquals(rs.getString("TABLE_NAME"), tableName);
                assertEquals(rs.getString("TABLE_TYPE"), "TABLE");
                assertEquals(rs.getString("REMARKS"), expectedRemarks);
                assertFalse(rs.next());
            }
        }
    }

    @DataProvider(name = "showStatementsFlagForRestrictedUser")
    public Object[][] showStatementsFlagForRestrictedUser() {
        return new Object[][] {
                {"true", Arrays.asList("metadata_show_statements_granted.id:Int32",
                        "metadata_show_statements_granted.t:Tuple(a Int32, b String)")},
                {"false", Arrays.asList("metadata_show_statements_granted.id:Int32",
                        "metadata_show_statements_granted.t:Tuple(a Int32, b String)",
                        "metadata_show_statements_restricted.id:Int32")},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "showStatementsFlagForRestrictedUser")
    public void testGetColumnsForReadonlyUserWithColumnGrants(String flag, List<String> expectedColumns) throws Exception {
        if (isCloud()) {
            throw new SkipException("Should not be tested in cloud because creates users");
        }
        final String db = getDatabase();
        final String user = "metadata_show_statements_user";
        final String password = UUID.randomUUID() + "Aa1!";
        try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS metadata_show_statements_granted");
            stmt.executeUpdate("DROP TABLE IF EXISTS metadata_show_statements_restricted");
            stmt.executeUpdate("CREATE TABLE metadata_show_statements_granted (id Int32, t Tuple(a Int32, b String)) " +
                    "ENGINE = MergeTree ORDER BY id");
            stmt.executeUpdate("CREATE TABLE metadata_show_statements_restricted (id Int32, secret String) " +
                    "ENGINE = MergeTree ORDER BY id");
            stmt.executeUpdate("DROP USER IF EXISTS " + user);
            stmt.executeUpdate("CREATE USER " + user + " IDENTIFIED WITH plaintext_password BY '" + password +
                    "' SETTINGS readonly = 1");
            stmt.executeUpdate("GRANT SELECT ON " + db + ".metadata_show_statements_granted TO " + user);
            stmt.executeUpdate("GRANT SELECT(id) ON " + db + ".metadata_show_statements_restricted TO " + user);
        }
        try {
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty(DriverProperties.METADATA_USE_SHOW_STATEMENTS.getKey(), flag);
            List<String> columns = new ArrayList<>();
            try (Connection conn = getJdbcConnection(props);
                 ResultSet rs = conn.getMetaData().getColumns(null, db, "metadata_show_statements_%ed", null)) {
                while (rs.next()) {
                    columns.add(rs.getString("TABLE_NAME") + "." + rs.getString("COLUMN_NAME") + ":" +
                            rs.getString("TYPE_NAME"));
                }
            }
            assertEquals(columns, expectedColumns);
        } finally {
            try (Connection conn = getJdbcConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP USER IF EXISTS " + user);
            }
        }
    }

    @DataProvider(name = "columnNameLikePatterns")
    public Object[][] columnNameLikePatterns() {
        return new Object[][] {
                {null, "any_name", true},
                {"", "a", false},
                {"%", "a\nb", true},
                {"abc", "abc", true},
                {"abc", "ABC", false},
                {"abc", "abcd", false},
                {"a_c", "abc", true},
                {"a_c", "ac", false},
                {"a%c", "ac", true},
                {"a%c", "a.b.c", true},
                {"a%c", "acd", false},
                {"a\\_c", "a_c", true},
                {"a\\_c", "abc", false},
                {"100\\%", "100%", true},
                {"100\\%", "1000", false},
                {"a\\\\b", "a\\b", true},
                {"a.c", "abc", false},
                {"(x)[y]*", "(x)[y]*", true},
                {"_", "😀", true},
        };
    }

    @Test(groups = {"integration"}, dataProvider = "columnNameLikePatterns")
    public void testGetColumnsMatchesColumnNamePatternLikeServer(String pattern, String columnName, boolean expected)
            throws Exception {
        final String tableName = "metadata_show_statements_like";
        try (Connection conn = getJdbcConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
                stmt.executeUpdate("CREATE TABLE " + tableName + " (" + quoteIdentifier(columnName) + " String) " +
                        "ENGINE = Memory");
            }
            if (pattern != null) {
                try (PreparedStatement stmt = conn.prepareStatement("SELECT ? LIKE ?")) {
                    stmt.setString(1, columnName);
                    stmt.setString(2, pattern);
                    try (ResultSet rs = stmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(rs.getBoolean(1), expected, "server: '" + columnName + "' LIKE '" + pattern + "'");
                    }
                }
            }
            try (ResultSet rs = conn.getMetaData().getColumns(null, getDatabase(), tableName, pattern)) {
                assertEquals(rs.next(), expected, "getColumns: '" + columnName + "' LIKE '" + pattern + "'");
                if (expected) {
                    assertEquals(rs.getString("COLUMN_NAME"), columnName);
                    assertFalse(rs.next());
                }
            }
        }
    }
}
