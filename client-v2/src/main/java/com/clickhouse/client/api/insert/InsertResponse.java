package com.clickhouse.client.api.insert;

import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;

import java.util.Collections;
import java.util.Map;

public class InsertResponse implements AutoCloseable {
    private OperationMetrics operationMetrics;
    private final Map<String, String> responseHeaders;

    public InsertResponse(OperationMetrics metrics) {
        this(metrics, Collections.emptyMap());
    }

    public InsertResponse(OperationMetrics metrics, Map<String, String> responseHeaders) {
        this.operationMetrics = metrics;
        this.responseHeaders = responseHeaders;
    }

    @Override
    public void close() {
    }

    /**
     * Returns the metrics of this operation.
     *
     * @return metrics of this operation
     */
    public OperationMetrics getMetrics() {
        return operationMetrics;
    }

    /**
     * Alias for {@link ServerMetrics#NUM_ROWS_READ}
     * @return number of rows read by server from the storage
     */
    public long getReadRows() {
        return operationMetrics.getMetric(ServerMetrics.NUM_ROWS_READ).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_BYTES_READ}
     * @return number of bytes read by server from the storage
     */
    public long getReadBytes() {
        return operationMetrics.getMetric(ServerMetrics.NUM_BYTES_READ).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_ROWS_WRITTEN}
     * @return number of rows written by server to the storage
     */
    public long getWrittenRows() {
        return operationMetrics.getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_BYTES_WRITTEN}
     * @return number of bytes written by server to the storage
     */
    public long getWrittenBytes() {
        return operationMetrics.getMetric(ServerMetrics.NUM_BYTES_WRITTEN).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#ELAPSED_TIME}
     * @return elapsed time in nanoseconds
     */
    public long getServerTime() {
        return operationMetrics.getMetric(ServerMetrics.ELAPSED_TIME).getLong();
    }

    /**
     * Alias for {@link ServerMetrics#RESULT_ROWS}
     * @return number of returned rows
     */
    public long getResultRows() {
        return operationMetrics.getMetric(ServerMetrics.RESULT_ROWS).getLong();
    }

    /**
     * Alias for {@link OperationMetrics#getQueryId()}
     * @return number of returned bytes
     */
    public String getQueryId() {
        return operationMetrics.getQueryId();
    }

    /**
     * Returns the value of {@code X-ClickHouse-Server-Display-Name} response header.
     *
     * @return server display name or {@code null} if not present
     */
    public String getServerDisplayName() {
        return responseHeaders.get(ClickHouseHttpProto.HEADER_SRV_DISPLAY_NAME);
    }

    /**
     * Returns all collected response headers as an unmodifiable map.
     * Only whitelisted ClickHouse headers are included.
     *
     * @return map of header name to header value
     */
    public Map<String, String> getResponseHeaders() {
        return responseHeaders;
    }
}
