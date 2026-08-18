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
import java.util.Objects;

/**
 * Starts spans on a {@link SpanRecorder} and records the client's standard set of attributes on
 * them.
 * <p>
 * All values reported through the observability SPI are computed here, so every recorder observes
 * the same information for the same operation. An implementation that wraps or replaces parts of
 * the client can reuse this class, or extend it to report additional attributes - every method may
 * be overridden.
 * <p>
 * A recorder is required; an application that records nothing registers
 * {@link DefaultSpanRecorder#NOOP} (the client's default). With that recorder the instance is
 * disabled: every method returns immediately and no attribute value is computed, so an application
 * that did not register a recorder pays only a boolean check.
 */
public class SpanSupport {

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
     * Value for the batch size when the client does not know how many rows it sends.
     */
    public static final int BATCH_SIZE_UNKNOWN = -1;

    /**
     * Key under which the statement parameters are kept in the request settings.
     */
    protected static final String KEY_STATEMENT_PARAMS = HttpAPIClientHelper.KEY_STATEMENT_PARAMS;

    /**
     * Shared instance that records nothing.
     */
    public static final SpanSupport DISABLED = new SpanSupport(DefaultSpanRecorder.NOOP);

    private final SpanRecorder recorder;

    private final boolean enabled;

    /**
     * Creates support for the given recorder.
     *
     * @param recorder - recorder registered by the application; {@link DefaultSpanRecorder#NOOP} to
     *                 record nothing. Must not be {@code null} - the client's default recorder
     *                 already records nothing, so a {@code null} here is a configuration error.
     * @throws NullPointerException when {@code recorder} is {@code null}
     */
    public SpanSupport(SpanRecorder recorder) {
        this.recorder = Objects.requireNonNull(recorder, "recorder is required; use DefaultSpanRecorder.NOOP to record nothing");
        this.enabled = recorder != DefaultSpanRecorder.NOOP;
    }

    /**
     * Returns whether a recorder is registered. When it is not, an operation should not do any
     * span-related work at all.
     *
     * @return {@code true} when spans are recorded
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Returns the registered recorder, or a recorder that records nothing when there is none.
     *
     * @return recorder; never {@code null}
     */
    protected SpanRecorder getRecorder() {
        return recorder;
    }

    /**
     * Starts an operation span for a query or a command. Every operation the client implements on
     * top of a query - a ping or a table-schema lookup - is reported as a query; a recorder that
     * wants to describe it differently derives that from the settings it is given.
     *
     * @param settings - resolved request settings
     * @param sqlQuery - statement sent to the server
     * @param endpoint - endpoint the operation is expected to use
     * @return operation span
     */
    public Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint) {
        if (!enabled) {
            return DefaultSpanRecorder.NOOP_SPAN;
        }

        final String namespace = settings.getDatabase();
        Span span = orNoop(recorder.startSpan(spanName(OPERATION_QUERY, namespace, null), settings));
        recordCommonAttributes(span, namespace, settings.getQueryId(), null, null,
                BATCH_SIZE_UNKNOWN, endpoint);
        span.setAttribute(SpanAttribute.DB_QUERY_TEXT.getKey(), sqlQuery);
        recordStatementParams(span, settings.getAllSettings());
        return span;
    }

    /**
     * Starts an operation span for an insert.
     *
     * @param settings - resolved request settings
     * @param tableName - target table
     * @param batchSize - number of items in the batch, or {@link #BATCH_SIZE_UNKNOWN} when the
     *                  client does not know it (stream and writer inserts)
     * @param endpoint - endpoint the operation is expected to use
     * @return operation span
     */
    public Span startInsertSpan(InsertSettings settings, String tableName, int batchSize, Endpoint endpoint) {
        if (!enabled) {
            return DefaultSpanRecorder.NOOP_SPAN;
        }

        final String namespace = settings.getDatabase();
        Span span = orNoop(recorder.startSpan(spanName(OPERATION_INSERT, namespace, tableName), settings));
        recordCommonAttributes(span, namespace, settings.getQueryId(), OPERATION_INSERT, tableName,
                batchSize, endpoint);
        return span;
    }

    /**
     * Starts a request span for a single transport attempt of an operation.
     *
     * @param operationSpan - span of the operation this request belongs to
     * @param host - server the request is sent to
     * @param port - port the request is sent to
     * @return request span
     */
    public Span startRequestSpan(Span operationSpan, String host, int port) {
        if (!enabled) {
            return DefaultSpanRecorder.NOOP_SPAN;
        }

        Span span = orNoop(recorder.startRequestSpan(REQUEST_SPAN_NAME, operationSpan));
        span.setAttribute(SpanAttribute.HTTP_REQUEST_METHOD.getKey(), REQUEST_SPAN_NAME);
        recordEndpoint(span, host, port);
        return span;
    }

    /**
     * Records the HTTP status code returned for a transport request.
     *
     * @param span - span of the request
     * @param statusCode - HTTP status code the server answered with
     */
    public void recordHttpStatus(Span span, int statusCode) {
        if (!enabled) {
            return;
        }
        span.setAttribute(SpanAttribute.HTTP_RESPONSE_STATUS_CODE.getKey(), statusCode);
    }

    /**
     * Records the server a request is sent to. Called per attempt, because a retry may go to another
     * node.
     *
     * @param span - span to record on
     * @param host - server hostname, or {@code null} when it is not known
     * @param port - server port, or a non-positive value when it is not known
     */
    public void recordEndpoint(Span span, String host, int port) {
        if (!enabled) {
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
     *
     * @param span - span of the operation
     * @param metrics - metrics of the completed operation, may be {@code null}
     */
    public void recordSuccess(Span span, OperationMetrics metrics) {
        if (!enabled || metrics == null) {
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
     *
     * @param span - span of the request
     * @param t - failure
     */
    public void recordRequestFailure(Span span, Throwable t) {
        if (!enabled || t == null) {
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
     *
     * @param span - span to record on
     * @param t - failure
     */
    public void recordFailure(Span span, Throwable t) {
        if (!enabled || t == null) {
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
     * Replaces a span a recorder did not return with one that records nothing, so that an
     * incomplete implementation cannot break an operation.
     */
    protected Span orNoop(Span span) {
        return span == null ? DefaultSpanRecorder.NOOP_SPAN : span;
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
