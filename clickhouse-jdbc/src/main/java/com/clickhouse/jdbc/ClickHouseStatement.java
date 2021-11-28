package com.clickhouse.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseRequest.Mutation;

public interface ClickHouseStatement extends Statement {
    @Override
    ClickHouseConnection getConnection() throws SQLException;

    ClickHouseConfig getConfig();

    ClickHouseRequest<?> getRequest();

    default Mutation write() {
        return getRequest().write();
    }
}
