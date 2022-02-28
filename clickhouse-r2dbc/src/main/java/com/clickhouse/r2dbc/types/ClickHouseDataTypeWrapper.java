package com.clickhouse.r2dbc.types;

import com.clickhouse.client.ClickHouseDataType;
import io.r2dbc.spi.Type;


public class ClickHouseDataTypeWrapper implements Type {
    ClickHouseDataType dType;

    private ClickHouseDataTypeWrapper(ClickHouseDataType dType){
        this.dType = dType;
    }

    public static ClickHouseDataTypeWrapper of(ClickHouseDataType dType) {
        return new ClickHouseDataTypeWrapper(dType);
    }


    @Override
    public Class getJavaType() {
        return dType.getObjectClass();
    }

    @Override
    public String getName() {
        return dType.name();
    }
}
