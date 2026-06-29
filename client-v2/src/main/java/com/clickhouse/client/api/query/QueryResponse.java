package com.clickhouse.client.api.query;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.transport.internal.TransportResponse;
import com.clickhouse.data.ClickHouseFormat;

import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

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

    private final ClickHouseFormat format;

    private final QuerySettings settings;

    private final OperationMetrics operationMetrics;

    private final TransportResponse transportResponse;

    private final Map<String, String> responseHeaders;

    public QueryResponse(TransportResponse response, ClickHouseFormat format, QuerySettings settings, OperationMetrics operationMetrics) {
        Objects.requireNonNull(response, "response is null");
        this.transportResponse = response;
        this.format = format;
        this.operationMetrics = operationMetrics;
        this.settings = settings;
        this.responseHeaders = response.getHeaders();

        String timeZoneHeader = responseHeaders.get(ClickHouseHttpProto.HEADER_TIMEZONE);
        if (timeZoneHeader != null) {
            TimeZone serverTz;
            try {
                serverTz = TimeZone.getTimeZone(timeZoneHeader);
            } catch (Exception e) {
                throw new ClientException("Failed to parse server timezone", e);
            }
            this.settings.setOption(ClientConfigProperties.SERVER_TIMEZONE.getKey(),
                    serverTz);
        }
    }

    public InputStream getInputStream() {
        return transportResponse.createDataInputStream();
    }

    @Override
    public void close() throws Exception {
        try {
            transportResponse.close();
        } catch (Exception e) {
            throw new ClientException("Failed to close response", e);
        }
    }

    public ClickHouseFormat getFormat() {
        return format;
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
     * Alias for {@link ServerMetrics#TOTAL_ROWS_TO_READ}
     * @return estimated number of rows to read
     */
    public long getTotalRowsToRead() {
        return operationMetrics.getMetric(ServerMetrics.TOTAL_ROWS_TO_READ).getLong();
    }

    /**
     * Alias for {@link OperationMetrics#getQueryId()}
     * @return query id of the request
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

    public TimeZone getTimeZone() {
        return settings.getOption(ClientConfigProperties.SERVER_TIMEZONE.getKey()) == null
                ? null
                : (TimeZone) settings.getOption(ClientConfigProperties.SERVER_TIMEZONE.getKey());
    }

    public QuerySettings getSettings() {
        return settings;
    }
}
