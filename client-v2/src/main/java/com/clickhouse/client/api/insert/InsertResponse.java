package com.clickhouse.client.api.insert;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.metrics.OperationMetrics;

public class InsertResponse implements AutoCloseable {
    private final ClickHouseResponse responseRef;
    private final ClickHouseClient client;

    private OperationMetrics operationMetrics;

    public InsertResponse(ClickHouseClient client, ClickHouseResponse responseRef,
                          ClientStatisticsHolder clientStatisticsHolder) {
        this.responseRef = responseRef;
        this.client = client;
        this.operationMetrics = new OperationMetrics(clientStatisticsHolder);
        this.operationMetrics.operationComplete(responseRef.getSummary());
    }

    @Override
    public void close() {
        try {
            responseRef.close();
        } finally {
            client.close();
        }
    }

    public OperationMetrics getMetrics() {
        return operationMetrics;
    }
}
