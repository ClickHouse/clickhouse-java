package com.clickhouse.client.api.insert;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.OperationStatistics;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InsertResponse implements AutoCloseable {
    private final ClickHouseResponse responseRef;
    private final ClickHouseClient client;

    private OperationStatistics operationStatistics;

    public InsertResponse(ClickHouseClient client, ClickHouseResponse responseRef, long startTimestamp) {
        this.responseRef = responseRef;
        this.client = client;
        this.operationStatistics = new OperationStatistics(startTimestamp);
        this.operationStatistics.updateServerStats(responseRef.getSummary());
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

    public OperationStatistics getOperationStatistics() {
        return operationStatistics;
    }
}
