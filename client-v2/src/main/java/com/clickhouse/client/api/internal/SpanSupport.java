package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.Metric;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.metrics.ServerMetrics;
import com.clickhouse.client.api.observability.Span;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.observability.SpanRecorder;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;

import java.util.Map;

/**
 * Starts spans and records the client's standard set of attributes on them.
 * <p>
 * All values reported through the observability SPI are computed here, so every
 * {@link SpanRecorder} implementation observes the same information for the same operation.
 * <p>
 * Every method returns immediately when no recorder is configured, so an application that did not
 * register a recorder pays only a reference comparison.
 */
public final class SpanSupport {

    /**
     * Value of {@link SpanAttribute#DB_SYSTEM_NAME}.
     */
    public static final String DB_SYSTEM_NAME = "clickhouse";

    public static final String OPERATION_QUERY = "query";

    public static final String OPERATION_INSERT = "insert";

    public static final String OPERATION_PING = "ping";

    public static final String OPERATION_GET_TABLE_SCHEMA = "getTableSchema";

    /**
     * Name of a transport request span. All requests the client makes are HTTP {@code POST}s.
     */
    private static final String REQUEST_SPAN_NAME = "POST";

    /**
     * Value for the batch size when the client does not know how many rows it sends.
     */
    public static final int BATCH_SIZE_UNKNOWN = -1;

    private SpanSupport() {
    }

    /**
     * Starts an operation span for a query, a command, a ping or a table-schema lookup.
     *
     * @param recorder - configured recorder
     * @param settings - resolved request settings
     * @param sqlQuery - statement sent to the server
     * @param operationName - value for {@link SpanAttribute#DB_OPERATION_NAME}; {@code null} for a
     *                      plain query or command
     * @param collectionName - target table; {@code null} when the operation has no single table
     * @param endpoint - endpoint known before the first attempt, or {@code null} when the client
     *                 may pick between several
     * @return operation span
     */
    public static Span startQuerySpan(SpanRecorder recorder, QuerySettings settings, String sqlQuery,
                                      String operationName, String collectionName, Endpoint endpoint) {
        if (recorder == SpanRecorder.NOOP) {
            return Span.NOOP;
        }

        final String namespace = settings.getDatabase();
        Span span = recorder.startSpan(spanName(operationName == null ? OPERATION_QUERY : operationName,
                namespace, collectionName), settings);
        recordCommonAttributes(span, namespace, settings.getQueryId(), operationName, collectionName,
                BATCH_SIZE_UNKNOWN, endpoint);
        if (operationName == null) {
            // the statement is reported for a query or a command; a named operation is described by
            // its operation name and target table instead
            span.setAttribute(SpanAttribute.DB_QUERY_TEXT.getKey(), sqlQuery);
            recordStatementParams(span, settings.getAllSettings());
        }
        return span;
    }

    /**
     * Starts an operation span for an insert.
     *
     * @param recorder - configured recorder
     * @param settings - resolved request settings
     * @param tableName - target table
     * @param batchSize - number of items in the batch, or {@code -1} when the client does not know
     *                  it (stream and writer inserts)
     * @param endpoint - endpoint known before the first attempt, or {@code null} when the client
     *                 may pick between several
     * @return operation span
     */
    public static Span startInsertSpan(SpanRecorder recorder, InsertSettings settings, String tableName,
                                       int batchSize, Endpoint endpoint) {
        if (recorder == SpanRecorder.NOOP) {
            return Span.NOOP;
        }

        final String namespace = settings.getDatabase();
        Span span = recorder.startSpan(spanName(OPERATION_INSERT, namespace, tableName), settings);
        recordCommonAttributes(span, namespace, settings.getQueryId(), OPERATION_INSERT, tableName,
                batchSize, endpoint);
        return span;
    }

    /**
     * Starts a request span for a single transport attempt of an operation.
     *
     * @param recorder - configured recorder
     * @param operationSpan - span of the operation this request belongs to
     * @param host - server the request is sent to
     * @param port - port the request is sent to
     * @return request span
     */
    public static Span startRequestSpan(SpanRecorder recorder, Span operationSpan, String host, int port) {
        if (recorder == SpanRecorder.NOOP) {
            return Span.NOOP;
        }

        Span span = recorder.startRequestSpan(REQUEST_SPAN_NAME, operationSpan);
        span.setAttribute(SpanAttribute.HTTP_REQUEST_METHOD.getKey(), REQUEST_SPAN_NAME);
        recordEndpoint(span, host, port);
        return span;
    }

    /**
     * Records the HTTP status code returned for a transport request.
     */
    public static void recordHttpStatus(Span span, int statusCode) {
        if (span == Span.NOOP) {
            return;
        }
        span.setAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE.getKey(), statusCode);
    }

    /**
     * Records the server the request is sent to. Called per attempt, because a retry may go to
     * another node.
     */
    public static void recordEndpoint(Span span, String host, int port) {
        if (span == Span.NOOP) {
            return;
        }
        if (host != null) {
            span.setAttribute(SpanAttribute.SERVER_ADDRESS.getKey(), host);
        }
        if (port > 0) {
            span.setAttribute(SpanAttribute.SERVER_PORT.getKey(), port);
        }
    }

    /**
     * Records the outcome of a successfully completed operation.
     */
    public static void recordSuccess(Span span, OperationMetrics metrics) {
        if (span == Span.NOOP || metrics == null) {
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
     * with an error response.
     */
    public static void recordRequestFailure(Span span, Throwable t) {
        if (span == Span.NOOP || t == null) {
            return;
        }

        ServerException serverException = findServerException(t);
        if (serverException != null && serverException.getTransportProtocolCode() > 0) {
            recordHttpStatus(span, serverException.getTransportProtocolCode());
        }
        recordFailure(span, t);
    }

    /**
     * Records the failure of an operation or of a single transport request. The ClickHouse error
     * code is recorded when the server reported one.
     */
    public static void recordFailure(Span span, Throwable t) {
        if (span == Span.NOOP || t == null) {
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

    private static void recordCommonAttributes(Span span, String namespace, String queryId, String operationName,
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

    @SuppressWarnings("unchecked")
    private static void recordStatementParams(Span span, Map<String, Object> settings) {
        Object params = settings.get(HttpAPIClientHelper.KEY_STATEMENT_PARAMS);
        if (!(params instanceof Map)) {
            return;
        }
        for (Map.Entry<String, String> param : ((Map<String, String>) params).entrySet()) {
            span.setAttribute(SpanAttribute.DB_QUERY_PARAMETER.getKey(param.getKey()), param.getValue());
        }
    }

    private static ServerException findServerException(Throwable t) {
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
     */
    private static String spanName(String operationName, String namespace, String collectionName) {
        boolean hasNamespace = namespace != null && !namespace.isEmpty();
        if (collectionName != null && !collectionName.isEmpty()) {
            return hasNamespace
                    ? operationName + " " + namespace + "." + collectionName
                    : operationName + " " + collectionName;
        }
        return hasNamespace ? operationName + " " + namespace : operationName;
    }
}
