package com.clickhouse.jdbc.metadata;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.internal.ExceptionUtils;
import com.clickhouse.jdbc.internal.JdbcUtils;

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
        this.columns = columns;
        this.schema = schema;
        this.catalog = catalog;
        this.tableName = tableName;
        this.typeClassMap = typeClassMap;
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
        return false; // no auto-incremental types
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        try {
            // TODO: should be in sync with DatabaseMetadata
            return getColumn(column).getDataType().isCaseSensitive();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true; // all columns are considered as searchable
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        try {
            return getColumn(column).isNullable() ? columnNullable : columnNoNulls;
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        try {
            return getColumn(column).getDataType().isSigned();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        try {
            return getColumn(column).getColumnName();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return getColumn(column).getColumnName();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return schema;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        try {
            return getColumn(column).getPrecision();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public int getScale(int column) throws SQLException {
        try {
            return getColumn(column).getScale();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return tableName;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return catalog;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        try {
            return JdbcUtils.convertToSqlType(getColumn(column).getDataType()).getVendorTypeNumber();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        try {
            return getColumn(column).getOriginalTypeName();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
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
