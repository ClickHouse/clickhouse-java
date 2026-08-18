package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;

/**
 * Backend-agnostic hook that lets an application observe client operations as spans.
 * <p>
 * A recorder is registered with
 * {@link com.clickhouse.client.api.Client.Builder#setSpanRecorder(SpanRecorder)}. It is the first
 * thing the client calls, and it is called with everything the client knows about the operation, so
 * an implementation is free to record whatever it needs and however it wants - including nothing.
 * <p>
 * Two kinds of spans are started:
 * <ul>
 *     <li><b>Operation span</b> - one per client operation, started with
 *     {@link #startQuerySpan(QuerySettings, String, Endpoint)} or
 *     {@link #startInsertSpan(InsertSettings, String, int, Endpoint)}. It is expected to join the
 *     caller's ambient trace, so client spans appear under the application's own span.</li>
 *     <li><b>Request span</b> - one per transport request made for the operation, including every
 *     retry, started with {@link #startRequestSpan(Span, String, int)}. It is a child of the
 *     operation span.</li>
 * </ul>
 * The client reports the outcome of what it started through the {@code record...} methods and finally
 * ends the span with {@link Span#end()}.
 * <p>
 * An implementation does not have to derive the standard span names and attributes itself: it may
 * call {@link SpanSupport}, which computes them - the keys listed in {@link SpanAttribute} - from the
 * same structures. That is opt-in; a recorder that wants to report something else, or in another
 * form, simply does not use it.
 * <p>
 * Implementations should extend {@link DefaultSpanRecorder} and override only what they care about;
 * the inherited methods record nothing, so a recorder keeps working when the client starts a kind of
 * span it does not know about.
 * <p>
 * A recorder is shared by all operations of a client instance and must be thread-safe.
 */
public interface SpanRecorder {

    /**
     * Batch size the client reports when it does not know how many rows an insert sends (stream and
     * writer inserts).
     */
    int BATCH_SIZE_UNKNOWN = -1;

    /**
     * Starts a span for a read operation - a query, a command, a ping or a table-schema lookup.
     *
     * @param settings - resolved settings of the operation; source of the target database, the query
     *                 id and the statement parameters
     * @param sqlQuery - statement sent to the server
     * @param endpoint - first configured endpoint, which the operation is expected to use; the endpoint
     *                 of each attempt is reported on its request span instead
     * @return new span; never {@code null}
     */
    Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint);

    /**
     * Starts a span for an insert operation.
     *
     * @param settings - resolved settings of the operation; source of the target database and the
     *                 query id
     * @param tableName - target table
     * @param batchSize - number of items in the batch, or {@link #BATCH_SIZE_UNKNOWN} when the client
     *                  does not know it
     * @param endpoint - first configured endpoint, which the operation is expected to use; the endpoint
     *                 of each attempt is reported on its request span instead
     * @return new span; never {@code null}
     */
    Span startInsertSpan(InsertSettings settings, String tableName, int batchSize, Endpoint endpoint);

    /**
     * Starts a span for a single transport request made for an operation. Called once per attempt, so
     * a retried operation produces several request spans.
     *
     * @param operationSpan - span of the operation this request belongs to; the returned span should
     *                      be its child
     * @param host - server the request is sent to, or {@code null} when it is not known
     * @param port - port the request is sent to, or a non-positive value when it is not known
     * @return new span; never {@code null}
     */
    Span startRequestSpan(Span operationSpan, String host, int port);

    /**
     * Reports the HTTP status of the response received for a transport request. Called as soon as a
     * response is received, also when the client maps that response onto a failure.
     *
     * @param requestSpan - span of the request
     * @param statusCode - HTTP status code the server answered with
     */
    void recordHttpStatus(Span requestSpan, int statusCode);

    /**
     * Reports that an operation completed successfully.
     *
     * @param operationSpan - span of the operation
     * @param metrics - metrics of the completed operation; source of the query id and of the number
     *                of returned rows. May be {@code null}
     */
    void recordSuccess(Span operationSpan, OperationMetrics metrics);

    /**
     * Reports that an operation failed.
     *
     * @param operationSpan - span of the operation
     * @param t - failure the caller receives
     */
    void recordFailure(Span operationSpan, Throwable t);

    /**
     * Reports that a single transport request failed. The operation itself may still succeed, because
     * the client retries.
     *
     * @param requestSpan - span of the request
     * @param t - failure of this attempt
     */
    void recordRequestFailure(Span requestSpan, Throwable t);
}
