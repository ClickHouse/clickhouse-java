package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QuerySettings;

/**
 * Backend-agnostic hook that lets an application observe client operations as spans.
 * <p>
 * A recorder is registered with
 * {@link com.clickhouse.client.api.Client.Builder#setSpanRecorder(SpanRecorder)}. When no recorder
 * is registered nothing is recorded and no span-related work is done at all.
 * <p>
 * Two kinds of spans are started:
 * <ul>
 *     <li><b>Operation span</b> - one per client operation (query, command, insert, ping,
 *     table-schema lookup), started with one of the {@code startSpan} methods. The operation
 *     settings are passed in so that an implementation can take from them whatever it needs
 *     (target database, query id, server settings, ...).</li>
 *     <li><b>Request span</b> - one per transport request made for the operation, including every
 *     retry, started with {@link #startRequestSpan(String, Span)}. It is a child of the operation
 *     span.</li>
 * </ul>
 * An operation span is expected to join the caller's ambient trace, so client spans appear under
 * the application's own span. Attribute keys are defined by {@link SpanAttribute} and the values
 * are supplied by the client, so all recorders report the same information.
 * <p>
 * Implementations should extend {@link DefaultSpanRecorder} and override only the kinds of spans
 * they care about; the inherited methods return a span that records nothing, so a recorder keeps
 * working when the client starts a kind of span it does not know about.
 * <p>
 * A recorder is shared by all operations of a client instance and must be thread-safe.
 */
public interface SpanRecorder {

    /**
     * Starts a span for a read operation - a query, a command, a ping or a table-schema lookup.
     *
     * @param spanName - operation span name
     * @param settings - settings of the operation being started
     * @return new span; never {@code null}
     */
    Span startSpan(String spanName, QuerySettings settings);

    /**
     * Starts a span for an insert operation.
     *
     * @param spanName - operation span name
     * @param settings - settings of the operation being started
     * @return new span; never {@code null}
     */
    Span startSpan(String spanName, InsertSettings settings);

    /**
     * Starts a span for a single transport request made for an operation. Called once per attempt,
     * so a retried operation produces several request spans.
     *
     * @param spanName - request span name
     * @param operationSpan - span of the operation this request belongs to; the returned span
     *                      should be its child
     * @return new span; never {@code null}
     */
    Span startRequestSpan(String spanName, Span operationSpan);
}
