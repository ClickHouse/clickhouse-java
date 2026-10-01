package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.ConnectionImpl;
import com.clickhouse.jdbc.internal.DetachedResultSet;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.sql.DatabaseMetaData.typeNoNulls;
import static java.sql.DatabaseMetaData.typeNullable;

/**
 * Reads the metadata of {@link DatabaseMetaDataImpl} with {@code SHOW DATABASES}, {@code SHOW TABLES} and
 * {@code DESCRIBE TABLE} instead of the system tables. These statements also return the databases and tables that the
 * system tables hide by default, for example the tables of {@code DataLakeCatalog} databases.
 * Used when {@link com.clickhouse.jdbc.DriverProperties#METADATA_USE_SHOW_STATEMENTS} is set.
 * <p>
 * The result sets have the same columns and values as the result sets of the system table queries, except that
 * {@code getTables()} returns {@code null} in {@code REMARKS} and {@code TYPE_SCHEM}: {@code SHOW TABLES} returns no
 * table comment and no database engine.
 */
final class ShowStatementsMetaData {
    private static final Logger log = LoggerFactory.getLogger(ShowStatementsMetaData.class);

    private static final List<ClickHouseColumn> GET_SCHEMAS_COLUMNS = ClickHouseColumn.parse(
            "TABLE_SCHEM String, TABLE_CATALOG String");

    private static final List<ClickHouseColumn> GET_TABLES_COLUMNS = ClickHouseColumn.parse(
            "TABLE_CAT String, TABLE_SCHEM String, TABLE_NAME String, TABLE_TYPE String, REMARKS Nullable(String), " +
            "TYPE_CAT Nullable(String), TYPE_SCHEM Nullable(String), TYPE_NAME Nullable(String), " +
            "SELF_REFERENCING_COL_NAME Nullable(String), REF_GENERATION Nullable(String)");

    private static final List<ClickHouseColumn> GET_COLUMNS_COLUMNS = ClickHouseColumn.parse(
            "TABLE_CAT String, TABLE_SCHEM String, TABLE_NAME String, COLUMN_NAME String, DATA_TYPE Int32, " +
            "TYPE_NAME String, COLUMN_SIZE Nullable(Int32), BUFFER_LENGTH Int32, DECIMAL_DIGITS Nullable(Int32), " +
            "NUM_PREC_RADIX Nullable(Int32), NULLABLE Int32, REMARKS String, COLUMN_DEF String, SQL_DATA_TYPE Int32, " +
            "SQL_DATETIME_SUB Int32, CHAR_OCTET_LENGTH Nullable(Int32), ORDINAL_POSITION Int32, IS_NULLABLE String, " +
            "SCOPE_CATALOG Nullable(String), SCOPE_SCHEMA Nullable(String), SCOPE_TABLE Nullable(String), " +
            "SOURCE_DATA_TYPE Nullable(Int16), IS_AUTOINCREMENT String, IS_GENERATEDCOLUMN String");

    private static final Set<ClickHouseDataType> INTEGER_TYPES = EnumSet.of(ClickHouseDataType.Bool,
            ClickHouseDataType.Int8, ClickHouseDataType.Int16, ClickHouseDataType.Int32, ClickHouseDataType.Int64,
            ClickHouseDataType.Int128, ClickHouseDataType.Int256, ClickHouseDataType.UInt8, ClickHouseDataType.UInt16,
            ClickHouseDataType.UInt32, ClickHouseDataType.UInt64, ClickHouseDataType.UInt128, ClickHouseDataType.UInt256);

    private static final Set<ClickHouseDataType> DECIMAL_TYPES = EnumSet.of(ClickHouseDataType.Decimal,
            ClickHouseDataType.Decimal32, ClickHouseDataType.Decimal64, ClickHouseDataType.Decimal128,
            ClickHouseDataType.Decimal256);

    private static final Map<String, Integer> TYPE_NAME_TO_BYTE_LENGTH = Arrays.stream(ClickHouseDataType.values())
            .filter(type -> type.getByteLength() > 0)
            .collect(Collectors.toMap(ClickHouseDataType::name, ClickHouseDataType::getByteLength));

    // UNKNOWN_TABLE, UNKNOWN_DATABASE, ACCESS_DENIED, DATALAKE_DATABASE_ERROR
    private static final Set<Integer> SKIPPED_OBJECT_ERROR_CODES = new HashSet<>(Arrays.asList(60, 81, 497, 736));

    // DESCRIBE indents named tuple elements on new lines when print_pretty_type_names is enabled. The setting is not
    // changed in the query because readonly=1 users cannot change settings.
    private static final Pattern PRETTY_TYPE_OPENING = Pattern.compile("\\(\n *");
    private static final Pattern PRETTY_TYPE_SEPARATOR = Pattern.compile(",\n *");

    private final ConnectionImpl connection;

    ShowStatementsMetaData(ConnectionImpl connection) {
        this.connection = connection;
    }

    /**
     * Returns the rows of {@link java.sql.DatabaseMetaData#getSchemas(String, String)}.
     */
    ResultSet getSchemas(String schemaPattern) throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();
        try {
            for (String database : showDatabases(schemaPattern)) {
                Map<String, Object> record = new HashMap<>();
                record.put("TABLE_SCHEM", database);
                record.put("TABLE_CATALOG", "");
                records.add(record);
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
        return DetachedResultSet.createFromRecords(records, GET_SCHEMAS_COLUMNS, connection.getDefaultCalendar());
    }

    /**
     * Returns the rows of {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}.
     */
    ResultSet getTables(String schemaPattern, String tableNamePattern, Set<String> requestedTypes) throws SQLException {
        // Like the system.tables query: temporary tables are not returned (they have no row in system.databases), and
        // when no known table type is requested, the types do not filter the tables.
        boolean filterByType = requestedTypes.stream().anyMatch(DatabaseMetaDataImpl.TABLE_TYPES::contains);
        List<Map<String, Object>> records = new ArrayList<>();
        try {
            for (String[] table : showTables(schemaPattern, tableNamePattern, false)) {
                Map<String, Object> record = new HashMap<>();
                record.put("TABLE_CAT", "");
                record.put("TABLE_SCHEM", table[0]);
                record.put("TABLE_NAME", table[1]);
                record.put(DatabaseMetaDataImpl.TABLE_TYPE_COL_IN_GET_TABLES, table[2]);
                record.put("REMARKS", null);
                record.put("TYPE_CAT", null);
                record.put("TYPE_SCHEM", null);
                record.put("TYPE_NAME", null);
                record.put("SELF_REFERENCING_COL_NAME", null);
                record.put("REF_GENERATION", null);
                DatabaseMetaDataImpl.TABLE_TYPE_MUTATOR.accept(record);
                if (!filterByType || requestedTypes.contains(record.get(DatabaseMetaDataImpl.TABLE_TYPE_COL_IN_GET_TABLES))) {
                    records.add(record);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
        return DetachedResultSet.createFromRecords(records, GET_TABLES_COLUMNS, connection.getDefaultCalendar());
    }

    /**
     * Returns the rows of {@link java.sql.DatabaseMetaData#getColumns(String, String, String, String)}.
     * The rows are in the JDBC order (TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION) without a sort: the server sorts the
     * result of SHOW DATABASES and SHOW TABLES by name, and DESCRIBE returns the columns in table order.
     */
    ResultSet getColumns(String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Predicate<String> columnNameMatcher = likeMatcher(columnNamePattern);
        List<Map<String, Object>> records = new ArrayList<>();
        try {
            for (String[] table : showTables(schemaPattern, tableNamePattern, true)) {
                try {
                    records.addAll(describeTable(table[0], table[1], columnNameMatcher));
                } catch (SQLException e) {
                    if (!isSkippedObjectError(e)) {
                        throw e;
                    }
                    log.debug("Skipped metadata of table {}", table[0].isEmpty() ? table[1] : table[0] + "." + table[1], e);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
        return DetachedResultSet.createFromRecords(records, GET_COLUMNS_COLUMNS, connection.getDefaultCalendar());
    }

    /**
     * Returns the names of the databases that match the pattern, sorted by name (the server sorts the result of
     * SHOW DATABASES).
     */
    private List<String> showDatabases(String schemaPattern) throws SQLException {
        List<String> databases = new ArrayList<>();
        if (schemaPattern != null && schemaPattern.isEmpty()) {
            return databases; // SHOW ignores an empty LIKE pattern, but it matches only an empty name
        }
        try (PreparedStatement stmt = DatabaseMetaDataImpl.prepareStatement(connection, "SHOW DATABASES LIKE ?")) {
            stmt.setString(1, likePattern(schemaPattern));
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            }
        }
        return databases;
    }

    /**
     * Returns {database, name, engine} of the tables that match the patterns. Temporary tables have an empty database,
     * the same as in system.tables, and are returned only when {@code includeTemporary} is set.
     */
    private List<String[]> showTables(String schemaPattern, String tableNamePattern, boolean includeTemporary)
            throws SQLException {
        List<String[]> tables = new ArrayList<>();
        if (tableNamePattern != null && tableNamePattern.isEmpty()) {
            return tables; // SHOW ignores an empty LIKE pattern, but it matches only an empty name
        }
        if (includeTemporary && likeMatcher(schemaPattern).test("")) {
            tables.addAll(showTablesOfDatabase("", tableNamePattern));
        }
        for (String database : showDatabases(schemaPattern)) {
            try {
                tables.addAll(showTablesOfDatabase(database, tableNamePattern));
            } catch (SQLException e) {
                if (!isSkippedObjectError(e)) {
                    throw e;
                }
                log.debug("Skipped metadata of database {}", database, e);
            }
        }
        return tables;
    }

    /**
     * Returns {database, name, engine} of the tables of one database, or of the temporary tables when the database is
     * empty. The server accepts only an identifier after {@code FROM}, so the database name is escaped by
     * {@link #quoteIdentifier(String)} and only the pattern is a parameter.
     */
    @SuppressWarnings({"squid:S2077"})
    private List<String[]> showTablesOfDatabase(String database, String tableNamePattern) throws SQLException {
        String sql = database.isEmpty() ? "SHOW FULL TEMPORARY TABLES LIKE ?"
                : "SHOW FULL TABLES FROM " + quoteIdentifier(database) + " LIKE ?";
        List<String[]> tables = new ArrayList<>();
        try (PreparedStatement stmt = DatabaseMetaDataImpl.prepareStatement(connection, sql)) {
            stmt.setString(1, likePattern(tableNamePattern));
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(new String[]{database, rs.getString("name"), rs.getString("engine")});
                }
            }
        }
        return tables;
    }

    /**
     * Returns the rows of {@code getColumns()} for the columns of one table that match the column name matcher. The
     * server accepts only an identifier after {@code DESCRIBE TABLE}, so the names are escaped by
     * {@link #quoteIdentifier(String)}. A temporary table has an empty database.
     */
    @SuppressWarnings({"squid:S2077"})
    private List<Map<String, Object>> describeTable(String database, String table, Predicate<String> columnNameMatcher)
            throws SQLException {
        String tableRef = database.isEmpty() ? quoteIdentifier(table)
                : quoteIdentifier(database) + "." + quoteIdentifier(table);
        List<Map<String, Object>> records = new ArrayList<>();
        try (PreparedStatement stmt = DatabaseMetaDataImpl.prepareStatement(connection, "DESCRIBE TABLE " + tableRef);
             ResultSet rs = stmt.executeQuery()) {
            Set<String> describeColumns = new HashSet<>();
            ResultSetMetaData describeMetaData = rs.getMetaData();
            for (int i = 1; i <= describeMetaData.getColumnCount(); i++) {
                describeColumns.add(describeMetaData.getColumnLabel(i));
            }
            int position = 0;
            while (rs.next()) {
                if ((describeColumns.contains("is_subcolumn") && rs.getInt("is_subcolumn") != 0)
                        || (describeColumns.contains("is_virtual") && rs.getInt("is_virtual") != 0)) {
                    continue;
                }
                position++;
                String columnName = rs.getString("name");
                if (columnNameMatcher.test(columnName)) {
                    records.add(createColumnRecord(database, table, columnName, position, rs.getString("type"),
                            describeColumns.contains("default_expression") ? rs.getString("default_expression") : "",
                            describeColumns.contains("comment") ? rs.getString("comment") : ""));
                }
            }
        }
        return records;
    }

    private static Map<String, Object> createColumnRecord(String database, String table, String column, int position,
                                                          String describedType, String defaultExpression,
                                                          String comment) {
        String type = PRETTY_TYPE_SEPARATOR.matcher(PRETTY_TYPE_OPENING.matcher(describedType).replaceAll("("))
                .replaceAll(", ");
        Map<String, Object> record = new HashMap<>();
        record.put("TABLE_CAT", "");
        record.put("TABLE_SCHEM", database);
        record.put("TABLE_NAME", table);
        record.put("COLUMN_NAME", column);
        record.put("TYPE_NAME", type);
        putColumnSizes(record, type);
        record.put("BUFFER_LENGTH", 0);
        record.put("NULLABLE", type.contains("Nullable(") ? typeNullable : typeNoNulls);
        record.put("REMARKS", comment);
        record.put("COLUMN_DEF", defaultExpression);
        record.put("SQL_DATA_TYPE", 0);
        record.put("SQL_DATETIME_SUB", 0);
        record.put("ORDINAL_POSITION", position);
        record.put("IS_NULLABLE", type.toUpperCase(Locale.ROOT).contains("NULLABLE") ? "YES" : "NO");
        record.put("SCOPE_CATALOG", null);
        record.put("SCOPE_SCHEMA", null);
        record.put("SCOPE_TABLE", null);
        record.put("SOURCE_DATA_TYPE", null);
        record.put("IS_AUTOINCREMENT", "NO");
        record.put("IS_GENERATEDCOLUMN", "NO");
        DatabaseMetaDataImpl.DATA_TYPE_VALUE_FUNCTION.accept(record);
        return record;
    }

    /**
     * Sets COLUMN_SIZE, DECIMAL_DIGITS, NUM_PREC_RADIX and CHAR_OCTET_LENGTH with the values that the system.columns
     * query returns. system.columns unwraps only Nullable (and the custom name of SimpleAggregateFunction), and reports
     * the bit width of integers, the precision and scale of decimals and the length of FixedString.
     */
    private static void putColumnSizes(Map<String, Object> record, String type) {
        Integer precision = null;
        Integer radix = null;
        Integer scale = null;
        Integer octetLength = null;
        try {
            ClickHouseColumn column = ClickHouseColumn.of("", type);
            if (column.getDataType() == ClickHouseDataType.SimpleAggregateFunction) {
                column = column.getNestedColumns().get(0);
            }
            ClickHouseDataType dataType = column.getDataType();
            if (!column.isLowCardinality()) {
                if (dataType == ClickHouseDataType.FixedString) {
                    octetLength = column.getPrecision();
                } else if (INTEGER_TYPES.contains(dataType)) {
                    precision = dataType.getByteLength() * Byte.SIZE;
                    radix = 2;
                    scale = 0;
                } else if (DECIMAL_TYPES.contains(dataType)) {
                    precision = column.getPrecision();
                    radix = 10;
                    scale = column.getScale();
                }
            }
        } catch (Exception e) {
            log.debug("Failed to parse column type: {}", type, e);
        }
        Integer byteLength = TYPE_NAME_TO_BYTE_LENGTH.get(type);
        record.put("COLUMN_SIZE", octetLength != null ? octetLength
                : byteLength != null ? byteLength : precision != null ? precision : 0);
        record.put("DECIMAL_DIGITS", scale == null || scale == 0 ? null : scale);
        record.put("NUM_PREC_RADIX", radix);
        record.put("CHAR_OCTET_LENGTH", octetLength);
    }

    private static String likePattern(String pattern) {
        return pattern == null ? "%" : pattern;
    }

    private static String quoteIdentifier(String name) {
        return '`' + name.replace("\\", "\\\\").replace("`", "\\`") + '`';
    }

    /**
     * Returns true for errors of a single database or table that must not fail the whole metadata call: the object
     * was dropped after it was listed, the user can list it but cannot describe it (for example, with column-level
     * grants), or a data lake catalog cannot read its metadata.
     */
    private static boolean isSkippedObjectError(SQLException e) {
        return SKIPPED_OBJECT_ERROR_CODES.contains(e.getErrorCode());
    }

    /**
     * Returns a matcher with the semantics of the ClickHouse LIKE operator: {@code %} matches any sequence of
     * characters, {@code _} matches one character and {@code \} escapes the next character. A {@code null} pattern
     * matches everything.
     */
    private static Predicate<String> likeMatcher(String pattern) {
        if (pattern == null) {
            return value -> true;
        }
        StringBuilder regex = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '%' || c == '_') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append(c == '%' ? ".*" : ".");
            } else {
                literal.append(c == '\\' && i + 1 < pattern.length() ? pattern.charAt(++i) : c);
            }
        }
        if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
        }
        Pattern compiled = Pattern.compile(regex.toString(), Pattern.DOTALL);
        return value -> compiled.matcher(value).matches();
    }
}
