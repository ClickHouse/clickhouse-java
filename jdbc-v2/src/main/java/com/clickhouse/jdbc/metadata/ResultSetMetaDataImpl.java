package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.google.common.collect.ImmutableList;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class ResultSetMetaDataImpl implements java.sql.ResultSetMetaData, JdbcV2Wrapper {

    private final List<ClickHouseColumn> columns;

    private final String schema;

    private final String catalog;

    private final String tableName;

    private final Map<ClickHouseDataType, Class<?>> typeClassMap;

    public ResultSetMetaDataImpl(List<ClickHouseColumn> columns, String schema, String catalog, String tableName,
                                 Map<ClickHouseDataType, Class<?>> typeClassMap) {
        this.columns = ImmutableList.copyOf(columns);
        this.schema = schema;
        this.catalog = catalog;
        this.tableName = tableName;
        this.typeClassMap = typeClassMap;
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
        return JdbcUtils.convertToSqlType(getColumn(column).getDataType()).getVendorTypeNumber();
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
        try {
            Class<?> columnClassType = typeClassMap.get(getColumn(column).getDataType());
            if (columnClassType == null) {
                columnClassType = Object.class;
            }
            return columnClassType.getName();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }
}
