package com.clickhouse.client.api.command;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QueryResponse;

public class CommandResponse implements AutoCloseable {

    private final QueryResponse response;

    public CommandResponse(QueryResponse response) {
        this.response = response;
        try {
            response.close();
        } catch (Exception e) {
            throw new ClientException("Failed to close underlying resource", e);
        }
    }

    /**
     * Returns the metrics of this operation.
     *
     * @return metrics of this operation
     */
    public OperationMetrics getMetrics() {
        return response.getMetrics();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_ROWS_READ}
     *
     * @return number of rows read by server from the storage
     */
    public long getReadRows() {
        return response.getReadRows();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_BYTES_READ}
     *
     * @return number of bytes read by server from the storage
     */
    public long getReadBytes() {
        return response.getReadBytes();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_ROWS_WRITTEN}
     *
     * @return number of rows written by server to the storage
     */
    public long getWrittenRows() {
        return response.getWrittenRows();
    }

    /**
     * Alias for {@link ServerMetrics#NUM_BYTES_WRITTEN}
     *
     * @return number of bytes written by server to the storage
     */
    public long getWrittenBytes() {
        return response.getWrittenBytes();
    }

    /**
     * Alias for {@link ServerMetrics#ELAPSED_TIME}
     *
     * @return elapsed time in nanoseconds
     */
    public long getServerTime() {
        return response.getServerTime();
    }

    @Override
    public void close() throws Exception {
        response.close();
    }
}
