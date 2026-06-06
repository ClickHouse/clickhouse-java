package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.sql.SQLType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetMetaDataImpl implements java.sql.ResultSetMetaData, JdbcV2Wrapper {

    private final List<ClickHouseColumn> columns;

    private final String schema;

    private final String catalog;

    private final String tableName;

    // Per-column type information resolved once at construction time. This is the single source of truth for
    // data binding: ResultSetImpl reuses it (see resolveColumnClass) instead of recomputing the mapping itself.
    private final List<ColumnTypeBinding> columnTypeBindings;
    private final Map<String, ColumnTypeBinding> columnTypeBindingsByName;

    public ResultSetMetaDataImpl(List<ClickHouseColumn> columns, String schema, String catalog, String tableName,
                                 Map<ClickHouseDataType, Class<?>> typeClassMap, Map<String, Class<?>> customTypeMap) {
        this.columns = ImmutableList.copyOf(columns);
        this.schema = schema;
        this.catalog = catalog;
        this.tableName = tableName;
        this.columnTypeBindings = buildColumnTypeBindings(this.columns,
                typeClassMap != null ? typeClassMap : JdbcUtils.DATA_TYPE_CLASS_MAP,
                customTypeMap != null ? customTypeMap : Collections.emptyMap());

        Map<String, ColumnTypeBinding> byName = new HashMap<>();
        for (ColumnTypeBinding binding : columnTypeBindings) {
            byName.put(binding.column.getColumnName(), binding); // keep last on duplicate names
        }
        this.columnTypeBindingsByName = Collections.unmodifiableMap(byName);
    }

    private static List<ColumnTypeBinding> buildColumnTypeBindings(List<ClickHouseColumn> columns,
                                                                   Map<ClickHouseDataType, Class<?>> typeClassMap,
                                                                   Map<String, Class<?>> customTypeMap) {
        ImmutableList.Builder<ColumnTypeBinding> bindings = ImmutableList.builder();
        for (ClickHouseColumn column : columns) {
            ClickHouseDataType dataType = column.getDataType();
            SQLType jdbcType = JdbcUtils.convertToSqlType(dataType);
            Class<?> defaultClass = typeClassMap.get(dataType);

            // A user supplied type map can override the Java class for a given ClickHouse/JDBC type.
            Class<?> customClass = customTypeMap.get(dataType.name());
            if (customClass == null) {
                customClass = customTypeMap.get(jdbcType.getName());
            }

            final Class<?> columnClass;
            final int columnType;
            if (customClass != null) {
                columnClass = customClass;
                // Keep getColumnType() consistent with the overridden class.
                SQLType mappedSqlType = JdbcUtils.CLASS_TO_SQL_TYPE_MAP.get(customClass);
                columnType = (mappedSqlType != null ? mappedSqlType : jdbcType).getVendorTypeNumber();
            } else {
                columnClass = defaultClass != null ? defaultClass : Object.class;
                columnType = jdbcType.getVendorTypeNumber();
            }
            bindings.add(new ColumnTypeBinding(column, jdbcType, defaultClass, columnType, columnClass));
        }
        return bindings.build();
    }

    /**
     * Immutable per-column type binding. Holds both the values reported by the metadata methods
     * ({@code columnType}/{@code columnClass}, with the connection type map applied) and the raw inputs
     * ({@code column}, {@code jdbcType}, {@code defaultClass}) used by {@link #resolveColumnClass} when a caller
     * supplies its own type map.
     */
    private static final class ColumnTypeBinding {
        private final ClickHouseColumn column;
        private final SQLType jdbcType;
        private final Class<?> defaultClass;
        private final int columnType;
        private final Class<?> columnClass;

        ColumnTypeBinding(ClickHouseColumn column, SQLType jdbcType, Class<?> defaultClass,
                          int columnType, Class<?> columnClass) {
            this.column = column;
            this.jdbcType = jdbcType;
            this.defaultClass = defaultClass;
            this.columnType = columnType;
            this.columnClass = columnClass;
        }

        public ClickHouseColumn getColumn() {
            return column;
        }

        public SQLType getJdbcType() {
            return jdbcType;
        }

        public Class<?> getColumnClass() {
            return columnClass;
        }

        public Class<?> getDefaultClass() {
            return defaultClass;
        }

        public int getColumnType() {
            return columnType;
        }
    }

    private ColumnTypeBinding getColumnTypeBinding(int column) throws SQLException {
        try {
            return columnTypeBindings.get(column - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index out of range: " + column, ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
    }

    /**
     * Resolves the Java class a value of the given column should be materialized as when read through
     * {@code getObject}. This centralizes the data-binding logic so {@link com.clickhouse.jdbc.ResultSetImpl}
     * can reuse it instead of maintaining a parallel implementation.
     *
     * @param columnName column name (label)
     * @param typeMap optional caller supplied type map (e.g. from {@code getObject(col, map)}); when {@code null}
     *                or empty the column's default class is used
     * @return target Java class, or {@code null} to indicate the value should be read as-is
     * @throws SQLException if the column does not exist
     */
    public Class<?> resolveColumnClass(String columnName, Map<String, Class<?>> typeMap) throws SQLException {
        ColumnTypeBinding binding = columnTypeBindingsByName.get(columnName);
        if (binding == null) {
            throw new SQLException("Column \"" + columnName + "\" does not exist.", ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }

        ClickHouseDataType dataType = binding.getColumn().getDataType();
        switch (dataType) {
            case Point:
            case Ring:
            case LineString:
            case Polygon:
            case MultiPolygon:
            case MultiLineString:
            case Geometry:
                return null; // read as is
            default:
                break;
        }

        if (typeMap == null || typeMap.isEmpty()) {
            return binding.getDefaultClass();
        }

        Class<?> resolved = typeMap.get(dataType.name());
        if (resolved == null) {
            resolved = typeMap.getOrDefault(binding.getJdbcType().getName(), binding.getDefaultClass());
        }
        return resolved;
    }

    private void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Column index out of range: " + column, ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
    }

    private ClickHouseColumn getColumn(int column) throws SQLException {
        try {
            return columns.get(column - 1);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("Column index out of range: " + column, ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // no auto-incremental types
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        // TODO: should be in sync with DatabaseMetadata
        return getColumn(column).getDataType().isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // all columns are considered as searchable
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumn(column).isNullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumn(column).getDataType().isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumnIndex(column);
        return 80;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).getColumnName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return schema;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkColumnIndex(column);
        return tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkColumnIndex(column);
        return catalog;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumnTypeBinding(column).columnType;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getOriginalTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkColumnIndex(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumnTypeBinding(column).getColumnClass().getName();
    }
}
