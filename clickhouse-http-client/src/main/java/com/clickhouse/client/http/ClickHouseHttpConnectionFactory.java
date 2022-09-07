package com.clickhouse.client.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;

public final class ClickHouseHttpConnectionFactory {
    public static ClickHouseHttpConnection createConnection(ClickHouseNode server, ClickHouseRequest<?> request,
            ExecutorService executor) throws IOException {
        return new HttpUrlConnectionImpl(server, request, executor);
    }

    private ClickHouseHttpConnectionFactory() {
    }
}
