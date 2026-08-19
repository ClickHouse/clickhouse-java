package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.HttpAPIClientHelper;
import com.clickhouse.client.api.metrics.Metric;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;

import java.util.Map;

/**
 * Derives the client's standard span names and attributes from the structures a
 * {@link SpanRecorder} is called with.
 * <p>
 * This class is a helper <b>for</b> a recorder, not a layer in front of one: the client always calls
 * the registered recorder first, and an implementation decides whether to use this class. Using it is
 * how a recorder reports the same names and the same {@link SpanAttribute} values as every other
 * recorder; a recorder that wants other values either overrides the method that computes them, or
 * does not use this class at all.
 * <p>
 * Every method may be overridden. {@link #DEFAULT} is a shared instance for implementations that keep
 * the standard behaviour - the class holds no state.
 */
public class SpanSupport {

    /**
     * Shared instance with the standard behaviour.
     */
    public static final SpanSupport DEFAULT = new SpanSupport();

    /**
     * Value of {@link SpanAttribute#DB_SYSTEM_NAME}.
     */
    public static final String DB_SYSTEM_NAME = "clickhouse";

    public static final String OPERATION_QUERY = "query";

    public static final String OPERATION_INSERT = "insert";

    /**
     * Name of a transport request span. All requests the client makes are HTTP {@code POST}s.
     */
    public static final String REQUEST_SPAN_NAME = "POST";

    /**
     * Key under which the statement parameters are kept in the request settings.
     */
    protected static final String KEY_STATEMENT_PARAMS = HttpAPIClientHelper.KEY_STATEMENT_PARAMS;

    /**
     * Returns the name of the span of a read operation, following the OpenTelemetry database span
     * naming convention.
     *
     * @param settings - settings of the operation
     * @return span name
     */
    public String querySpanName(QuerySettings settings) {
        return spanName(OPERATION_QUERY, settings.getDatabase(), null);
    }

    /**
     * Returns the name of the span of an insert operation, following the OpenTelemetry database span
     * naming convention.
     *
     * @param settings - settings of the operation
     * @param tableName - target table
     * @return span name
     */
    public String insertSpanName(InsertSettings settings, String tableName) {
        return spanName(OPERATION_INSERT, settings.getDatabase(), tableName);
    }

    /**
     * Returns the name of the span of a single transport request.
     *
     * @return span name
     */
    public String requestSpanName() {
        return REQUEST_SPAN_NAME;
    }

    /**
     * Records the attributes of a read operation - a query, a command, a ping or a table-schema
     * lookup. Every operation the client implements on top of a query is described as a query; a
     * recorder that wants to describe one of them differently derives that from the settings and the
     * statement it is given.
     *
     * @param span - span of the operation
     * @param settings - resolved settings of the operation
     * @param sqlQuery - statement sent to the server
     * @param endpoint - endpoint the operation is expected to use; may be {@code null}
     */
    public void fillQueryAttributes(Span span, QuerySettings settings, String sqlQuery, Endpoint endpoint) {
        recordCommonAttributes(span, settings.getDatabase(), settings.getQueryId(), null, null,
                SpanRecorder.BATCH_SIZE_UNKNOWN, endpoint);
        span.setAttribute(SpanAttribute.DB_QUERY_TEXT.getKey(), sqlQuery);
        recordStatementParams(span, settings.getAllSettings());
    }

    /**
     * Records the attributes of an insert operation.
     *
     * @param span - span of the operation
     * @param settings - resolved settings of the operation
     * @param tableName - target table
     * @param batchSize - number of items in the batch, or {@link SpanRecorder#BATCH_SIZE_UNKNOWN}
     *                  when the client does not know it
     * @param endpoint - endpoint the operation is expected to use; may be {@code null}
     */
    public void fillInsertAttributes(Span span, InsertSettings settings, String tableName, int batchSize,
                                     Endpoint endpoint) {
        recordCommonAttributes(span, settings.getDatabase(), settings.getQueryId(), OPERATION_INSERT,
                tableName, batchSize, endpoint);
    }

    /**
     * Records the attributes of a single transport request.
     *
     * @param span - span of the request
     * @param host - server the request is sent to, or {@code null} when it is not known
     * @param port - port the request is sent to, or a non-positive value when it is not known
     */
    public void fillRequestAttributes(Span span, String host, int port) {
        span.setAttribute(SpanAttribute.HTTP_REQUEST_METHOD.getKey(), REQUEST_SPAN_NAME);
        recordEndpoint(span, host, port);
    }

    /**
     * Records the HTTP status returned for a transport request.
     *
     * @param span - span of the request
     * @param statusCode - HTTP status code the server answered with
     */
    public void recordHttpStatus(Span span, int statusCode) {
        span.setAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE.getKey(), statusCode);
    }

    /**
     * Records the server a request is sent to. Recorded per attempt, because a retry may go to
     * another node.
     *
     * @param span - span to record on
     * @param host - server hostname, or {@code null} when it is not known
     * @param port - server port, or a non-positive value when it is not known
     */
    public void recordEndpoint(Span span, String host, int port) {
        if (host != null) {
            span.setAttribute(SpanAttribute.SERVER_ADDRESS.getKey(), host);
        }
        if (port > 0) {
            span.setAttribute(SpanAttribute.SERVER_PORT.getKey(), port);
        }
    }

    /**
     * Records the outcome of a successfully completed operation.
     *
     * @param span - span of the operation
     * @param metrics - metrics of the completed operation, may be {@code null}
     */
    public void recordSuccess(Span span, OperationMetrics metrics) {
        if (metrics == null) {
            return;
        }

        if (metrics.getQueryId() != null) {
            span.setAttribute(SpanAttribute.CLICKHOUSE_QUERY_ID.getKey(), metrics.getQueryId());
        }
        // the row count comes from the server's progress summary, which is not always available
        Metric returnedRows = metrics.getMetric(ServerMetrics.RESULT_ROWS);
        if (returnedRows != null && returnedRows.getLong() >= 0) {
            span.setAttribute(SpanAttribute.DB_RESPONSE_RETURNED_ROWS.getKey(), returnedRows.getLong());
        }
    }

    /**
     * Records the failure of a single transport request. In addition to
     * {@link #recordFailure(Span, Throwable)} the HTTP status is recorded when the server answered
     * with an error response that carries it.
     *
     * @param span - span of the request
     * @param t - failure
     */
    public void recordRequestFailure(Span span, Throwable t) {
        if (t == null) {
            return;
        }

        ServerException serverException = findServerException(t);
        if (serverException != null && serverException.getTransportProtocolCode() > 0) {
            recordHttpStatus(span, serverException.getTransportProtocolCode());
        }
        recordFailure(span, t);
    }

    /**
     * Records the failure of an operation or of a single transport request. The ClickHouse error code
     * is recorded when the server reported one.
     *
     * @param span - span to record on
     * @param t - failure
     */
    public void recordFailure(Span span, Throwable t) {
        if (t == null) {
            return;
        }

        ServerException serverException = findServerException(t);
        if (serverException == null) {
            span.setError(t.getClass().getName());
        } else {
            span.setAttribute(SpanAttribute.DB_RESPONSE_STATUS_CODE.getKey(), serverException.getCode());
            span.setError(serverException.getClass().getName());
        }
    }

    /**
     * Records the attributes that describe the operation itself.
     */
    protected void recordCommonAttributes(Span span, String namespace, String queryId, String operationName,
                                          String collectionName, int batchSize, Endpoint endpoint) {
        span.setAttribute(SpanAttribute.DB_SYSTEM_NAME.getKey(), DB_SYSTEM_NAME);
        if (namespace != null) {
            span.setAttribute(SpanAttribute.DB_NAMESPACE.getKey(), namespace);
        }
        if (queryId != null) {
            span.setAttribute(SpanAttribute.CLICKHOUSE_QUERY_ID.getKey(), queryId);
        }
        if (operationName != null) {
            span.setAttribute(SpanAttribute.DB_OPERATION_NAME.getKey(), operationName);
        }
        if (collectionName != null) {
            span.setAttribute(SpanAttribute.DB_COLLECTION_NAME.getKey(), collectionName);
        }
        if (batchSize >= 0) {
            span.setAttribute(SpanAttribute.DB_OPERATION_BATCH_SIZE.getKey(), batchSize);
        }
        if (endpoint != null) {
            recordEndpoint(span, endpoint.getHost(), endpoint.getPort());
        }
    }

    /**
     * Records the values of the statement parameters sent with a query.
     */
    @SuppressWarnings("unchecked")
    protected void recordStatementParams(Span span, Map<String, Object> settings) {
        Object params = settings.get(KEY_STATEMENT_PARAMS);
        if (!(params instanceof Map)) {
            return;
        }
        for (Map.Entry<String, String> param : ((Map<String, String>) params).entrySet()) {
            span.setAttribute(SpanAttribute.DB_QUERY_PARAMETER.getKey(param.getKey()), param.getValue());
        }
    }

    /**
     * Finds the server error in a failure, if the server reported one.
     */
    protected ServerException findServerException(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof ServerException) {
                return (ServerException) cause;
            }
            if (cause.getCause() == cause) {
                break;
            }
        }
        return null;
    }

    /**
     * Builds a span name out of the operation name and its target, following the OpenTelemetry
     * database span naming convention - {@code <operation> <namespace>.<collection>}.
     *
     * @param operationName - name of the operation
     * @param namespace - target database, may be {@code null}
     * @param collectionName - target table, may be {@code null}
     * @return span name
     */
    protected String spanName(String operationName, String namespace, String collectionName) {
        boolean hasNamespace = namespace != null && !namespace.isEmpty();
        if (collectionName != null && !collectionName.isEmpty()) {
            return hasNamespace
                    ? operationName + " " + namespace + "." + collectionName
                    : operationName + " " + collectionName;
        }
        return hasNamespace ? operationName + " " + namespace : operationName;
    }
}
