package ru.yandex.clickhouse.response;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ClickHouseResultSetMetaData implements ResultSetMetaData {

    private final ClickHouseResultSet resultSet;

    ClickHouseResultSetMetaData(ClickHouseResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getColumns().size();
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
        return getCol(column).isNullable()
            ? columnNullable
            : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getCol(column).getClickHouseDataType().isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getCol(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getCol(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getCol(column).getScale();
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
        return getCol(column).getClickHouseDataType().getSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getCol(column).getCleanTypeName();
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
        return getCol(column).getClickHouseDataType().getJavaClass().getCanonicalName();
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

    private ClickHouseColumnInfo getCol(int column) {
        return resultSet.getColumns().get(column - 1);
    }

}
