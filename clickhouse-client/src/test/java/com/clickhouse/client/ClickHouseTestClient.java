package com.clickhouse.client;

import java.util.concurrent.CompletableFuture;

public class ClickHouseTestClient implements ClickHouseClient {
    private ClickHouseConfig clientConfig;

    @Override
    public boolean accept(ClickHouseProtocol protocol) {
        return protocol == ClickHouseProtocol.MYSQL; // to avoid taking this client in other tests
    }

    @Override
    public CompletableFuture<ClickHouseResponse> execute(ClickHouseRequest<?> request) {
        return CompletableFuture.supplyAsync(() -> null);
    }

    @Override
    public ClickHouseConfig getConfig() {
        return this.clientConfig;
    }

    @Override
    public void init(ClickHouseConfig config) {
        ClickHouseClient.super.init(config);

        this.clientConfig = config;
    }

    @Override
    public void close() {
        this.clientConfig = null;
    }
}
