package com.clickhouse.jdbc.metadata;

import java.sql.SQLException;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.JdbcV2Wrapper;
import com.clickhouse.jdbc.ResultSetImpl;
import com.clickhouse.jdbc.internal.JdbcUtils;
import com.clickhouse.jdbc.internal.ExceptionUtils;

public class ResultSetMetaData implements java.sql.ResultSetMetaData, JdbcV2Wrapper {
    private final ResultSetImpl resultSet;
    public ResultSetMetaData(ResultSetImpl resultSet) {
        this.resultSet = resultSet;
    }

    private ClickHouseColumn getColumn(int column) throws SQLException {
        if (column < 1 || column > getColumnCount()) {
            throw new SQLException("Column index out of range: " + column, ExceptionUtils.SQL_STATE_CLIENT_ERROR);
        }
        return resultSet.getSchema().getColumns().get(column - 1);
    }

    @Override
    public int getColumnCount() throws SQLException {
        try {
            TableSchema schema = resultSet.getSchema();
            return schema.getColumns().size();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
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
        return "";
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
        try {
            return resultSet.getSchema().getTableName();
        } catch (Exception e) {
            throw ExceptionUtils.toSqlState(e);
        }
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
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
        return null;
    }
}
