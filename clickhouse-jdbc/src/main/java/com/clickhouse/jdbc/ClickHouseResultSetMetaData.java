package com.clickhouse.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseUtils;

public class ClickHouseResultSetMetaData extends JdbcWrapper implements ResultSetMetaData {
    public static ResultSetMetaData of(String database, String table, List<ClickHouseColumn> columns)
            throws SQLException {
        if (database == null || table == null || columns == null) {
            throw SqlExceptionUtils.clientError("Non-null database, table, and column list are required");
        }

        return new ClickHouseResultSetMetaData(database, table, columns);
    }

    private final String database;
    private final String table;
    private final List<ClickHouseColumn> columns;

    protected ClickHouseResultSetMetaData(String database, String table, List<ClickHouseColumn> columns) {
        this.database = database;
        this.table = table;
        this.columns = columns;
    }

    protected List<ClickHouseColumn> getColumns() {
        return this.columns;
    }

    protected ClickHouseColumn getColumn(int index) throws SQLException {
        if (index < 1 || index > columns.size()) {
            throw SqlExceptionUtils.clientError(
                    ClickHouseUtils.format("Column index must between 1 and %d but we got %d", columns.size(), index));
        }
        return columns.get(index - 1);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
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
        return getColumn(column).isNullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumn(column).getDataType().isSigned();
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
        return getColumn(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return database;
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
        return table;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return JdbcTypeMapping.toJdbcType(getColumn(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getOriginalTypeName();
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
        return getColumn(column).getDataType().getObjectClass().getCanonicalName();
    }
}
