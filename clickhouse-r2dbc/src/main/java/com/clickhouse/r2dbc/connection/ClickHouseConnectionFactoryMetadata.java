package com.clickhouse.r2dbc.connection;

import io.r2dbc.spi.ConnectionFactoryMetadata;

public class ClickHouseConnectionFactoryMetadata implements ConnectionFactoryMetadata {

    static final ClickHouseConnectionFactoryMetadata INSTANCE = new ClickHouseConnectionFactoryMetadata();

    @Override
    public String getName() {
        return "ClickHouse";
    }
}
