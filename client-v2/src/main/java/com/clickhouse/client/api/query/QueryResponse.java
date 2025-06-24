package com.clickhouse.client.api.query;

import com.clickhouse.client.api.ClientConfigProperties;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.http.ClickHouseHttpProto;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.data.ClickHouseFormat;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;

import java.io.InputStream;
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

    private QuerySettings settings;

    private OperationMetrics operationMetrics;

    private ClassicHttpResponse httpResponse;

    public QueryResponse(ClassicHttpResponse response, ClickHouseFormat format, QuerySettings settings,
                         OperationMetrics operationMetrics) {
        this.httpResponse = response;
        this.format = format;
        this.operationMetrics = operationMetrics;
        this.settings = settings;

        Header tzHeader = response.getFirstHeader(ClickHouseHttpProto.HEADER_TIMEZONE);
        if (tzHeader != null) {
            try {
                this.settings.setOption(ClientConfigProperties.SERVER_TIMEZONE.getKey(),
                        TimeZone.getTimeZone(tzHeader.getValue()));
            } catch (Exception e) {
                throw new ClientException("Failed to parse server timezone", e);
            }
        }
    }

    public InputStream getInputStream() {
        try {
            return httpResponse.getEntity().getContent();
        } catch (Exception e) {
            throw new ClientException("Failed to construct input stream", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (httpResponse != null ) {
            try {
                httpResponse.close();
            } catch (Exception e) {
                throw new ClientException("Failed to close response", e);
            }
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

    public TimeZone getTimeZone() {
        return settings.getOption(ClientConfigProperties.SERVER_TIMEZONE.getKey()) == null
                ? null
                : (TimeZone) settings.getOption(ClientConfigProperties.SERVER_TIMEZONE.getKey());
    }

    public QuerySettings getSettings() {
        return settings;
    }
}
