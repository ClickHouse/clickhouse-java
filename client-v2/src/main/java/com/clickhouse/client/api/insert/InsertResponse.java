package com.clickhouse.client.api.insert;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class InsertResponse {
    private Future<ClickHouseResponse> responseRef;
    private ClickHouseClient client;

    public InsertResponse(ClickHouseClient client, Future<ClickHouseResponse> responseRef) {
        this.responseRef = responseRef;
        this.client = client;
    }

    public boolean isDone() {
        return responseRef.isDone();
    }

    public ClickHouseResponseSummary getSummary() throws ExecutionException, InterruptedException {
        return responseRef.get().getSummary();
    }
}
