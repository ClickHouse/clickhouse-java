package ru.yandex.clickhouse.response;

import ru.yandex.clickhouse.util.TypeUtils;

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
        String type = resultSet.getTypes()[column - 1];
        if (type.matches("Nullable\\(.*?\\)"))
            return columnNullable;
        else
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
        return TypeUtils.toSqlType(getColumnTypeName(column));
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
        String columnTypeName = getColumnTypeName(column);
        int sqlType = TypeUtils.toSqlType(columnTypeName);
        if (sqlType == Types.ARRAY) {
            String elementTypeName = TypeUtils.getArrayElementTypeName(columnTypeName);
            return TypeUtils.toClass(sqlType, TypeUtils.toSqlType(elementTypeName), TypeUtils.isUnsigned(elementTypeName)).getName();
        }
        return TypeUtils.toClass(sqlType, -1, TypeUtils.isUnsigned(columnTypeName)).getName();
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
