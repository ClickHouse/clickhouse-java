package ru.yandex.clickhouse.response;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;


public class ClickHouseResultSetMetaData implements ResultSetMetaData {

    private final ClickHouseResultSet resultSet;

    public ClickHouseResultSetMetaData(ClickHouseResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getColumnNames().length;
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
        return columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return !resultSet.getTypes()[column - 1].startsWith("U");
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return resultSet.getColumnNames()[column - 1];
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return resultSet.getColumnNames()[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return resultSet.getTable();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return resultSet.getDb();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return ClickHouseResultSet.toSqlType(getColumnTypeName(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (resultSet.getTypes().length < column) {
            throw new ArrayIndexOutOfBoundsException("Array length: " + resultSet.getTypes().length + " requested: " + (column - 1));
        }
        return resultSet.getTypes()[column - 1];
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
        int sqlType = getColumnType(column);
        switch (sqlType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Boolean.class.getName();
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.DOUBLE:
                return Double.class.getName();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class.getName();
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BLOB:
                return String.class.getName();
            case Types.FLOAT:
            case Types.REAL:
                return Float.class.getName();
            case Types.DATE:
                return java.sql.Date.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.TIME:
                return Time.class.getName();
            default:
                throw new UnsupportedOperationException("no classes for now");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("Unable to unwrap to " + iface.toString());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }
}
