package com.clickhouse.client.api.insert;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;

public class InsertResponse implements AutoCloseable {
    private final ClickHouseResponse responseRef;
    private final ClickHouseClient client;

    public InsertResponse(ClickHouseClient client, ClickHouseResponse responseRef) {
        this.responseRef = responseRef;
        this.client = client;
    }

    public ClickHouseResponseSummary getSummary() {
        return responseRef.getSummary();
    }

    @Override
    public void close() {
        try {
            responseRef.close();
        } finally {
            client.close();
        }
    }
}
