package com.clickhouse.jdbc.internal;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.jdbc.ResultSetImpl;

import java.sql.SQLException;

public class MetadataResultSet extends ResultSetImpl {
    public MetadataResultSet(ResultSetImpl resultSet) {
        super(resultSet);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        String value = super.getString(columnLabel);

        try {
            int val = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            //We only do this if it's not a number, so we can assume it's a type string
            if ("DATA_TYPE".equalsIgnoreCase(columnLabel)) {
                value = String.valueOf(JdbcUtils.convertToSqlType(ClickHouseColumn.parse("DATA_TYPE " + value).get(0).getDataType()).getVendorTypeNumber());
            }
        }

        return value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getString(columnLabel) == null ? 0 : Integer.parseInt(getString(columnLabel));
    }

}
