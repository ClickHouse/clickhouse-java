package com.clickhouse.client.api.query;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.internal.ClientV1AdaptorHelper;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.http.ClickHouseHttpProto;
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

    private final ClickHouseResponse clickHouseResponse;
    private final ClickHouseFormat format;
    private ClickHouseClient client;

    private QuerySettings settings;

    private OperationMetrics operationMetrics;

    private ClassicHttpResponse httpResponse;

    @Deprecated
    public QueryResponse(ClickHouseClient client, ClickHouseResponse clickHouseResponse,
                         QuerySettings settings, ClickHouseFormat format,
                         ClientStatisticsHolder clientStatisticsHolder) {
        this.client = client;
        this.clickHouseResponse = clickHouseResponse;
        this.format = format;
        this.settings = settings;
        this.operationMetrics = new OperationMetrics(clientStatisticsHolder);
        this.operationMetrics.operationComplete();
        this.operationMetrics.setQueryId(clickHouseResponse.getSummary().getQueryId());
        ClientV1AdaptorHelper.setServerStats(clickHouseResponse.getSummary().getProgress(),
                this.operationMetrics);
        settings.setOption("server_timezone", clickHouseResponse.getTimeZone());
    }

    public QueryResponse(ClassicHttpResponse response, QuerySettings settings, OperationMetrics operationMetrics) {
        this.clickHouseResponse = null;
        this.httpResponse = response;
        this.format = settings.getFormat();
        this.settings = settings;
        this.operationMetrics = operationMetrics;

        Header tzHeader = response.getFirstHeader(ClickHouseHttpProto.HEADER_TIMEZONE);
        if (tzHeader != null) {
            try {
                settings.setOption("server_timezone", TimeZone.getTimeZone(tzHeader.getValue()));
            } catch (Exception e) {
                throw new ClientException("Failed to parse server timezone", e);
            }
        }
    }

    public InputStream getInputStream() {
        if (clickHouseResponse != null) {
            try {
                return clickHouseResponse.getInputStream();
            } catch (Exception e) {
                throw new RuntimeException(e); // TODO: handle exception
            }
        } else {
            try {
                return httpResponse.getEntity().getContent();
            } catch (Exception e) {
                throw new ClientException("Failed to construct input stream", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (clickHouseResponse != null) {
            try {
                clickHouseResponse.close();
            } catch (Exception e) {
                throw new ClientException("Failed to close response", e);
            }
        }

        if (httpResponse != null ) {
            try {
                httpResponse.close();
            } catch (Exception e) {
                throw new ClientException("Failed to close response", e);
            }
        }

        if (client !=null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new ClientException("Failed to close client", e);
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
     * Alias for {@link OperationMetrics#getQueryId()}
     * @return query id of the request
     */
    public String getQueryId() {
        return operationMetrics.getQueryId();
    }

    public TimeZone getTimeZone() {
        return settings.getOption("server_timezone") == null
                ? null
                : (TimeZone) settings.getOption("server_timezone");
    }
}
