package com.clickhouse.r2dbc;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.r2dbc.types.ClickHouseDataTypeWrapper;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Type;

public class ClickHouseColumnMetadata implements ColumnMetadata {

    final Type type;
    final String name;

    ClickHouseColumnMetadata(ClickHouseColumn col) {
        this.name = col.getColumnName(); // TODO :check alias handling.
        this.type = ClickHouseDataTypeWrapper.of(col.getDataType());
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public String getName() {
        return name;
    }
}
