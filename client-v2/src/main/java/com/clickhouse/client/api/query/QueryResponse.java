package com.clickhouse.client.api.query;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.OperationStatistics;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseInputStream;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Response class provides interface to input stream of response data.
 * <br/>
 * It is used to read data from ClickHouse server.
 * It is used to get response metadata like errors, warnings, etc.
 * <p>
 * This class is for the following user cases:
 * <ul>
 *     <li>Full read. User does conversion from record to custom object</li>
 *     <li>Full read. No conversion to custom object. List of generic records is returned. </li>
 *     <li>Iterative read. One record is returned at a time</li>
 * </ul>
 */
public class QueryResponse implements AutoCloseable {

    private final Future<ClickHouseResponse> responseRef;
    private final ClickHouseFormat format;

    private long completeTimeout = TimeUnit.MINUTES.toMillis(1);

    private ClickHouseClient client;

    private QuerySettings settings;

    private OperationStatistics operationStatistics;

    public QueryResponse(ClickHouseClient client, Future<ClickHouseResponse> responseRef,
                         QuerySettings settings, ClickHouseFormat format) {
        this.client = client;
        this.responseRef = responseRef;
        this.format = format;
        this.settings = settings;

    }

    public boolean isDone() {
        return responseRef.isDone();
    }

    public void ensureDone() {
        if (!isDone()) {
            try {
                responseRef.get(completeTimeout, TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                throw new RuntimeException(e); // TODO: handle exception
            }
        }
    }

    public ClickHouseInputStream getInputStream() {
        ensureDone();
        try {
            return responseRef.get().getInputStream();
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO: handle exception
        }
    }

    @Override
    public void close() throws Exception {
        try {
            client.close();
        } catch (Exception e) {
            // TODO: decide about logging
        }
    }

    public ClickHouseFormat getFormat() {
        return format;
    }

    public OperationStatistics getOperationStatistics() {
        if (operationStatistics == null) {
            try {
                ensureDone();
                ClickHouseResponse response = responseRef.get();
                this.operationStatistics = new OperationStatistics(response.getSummary());
            } catch (Exception e) {
                throw new ClientException("Failed to load statistics", e);
            }
        }
        return operationStatistics;
    }
}
