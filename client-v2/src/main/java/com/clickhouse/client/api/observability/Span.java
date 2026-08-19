package com.clickhouse.client.api.observability;

/**
 * A single unit of work observed by a {@link SpanRecorder} - either a client operation or one
 * transport request made for it.
 * <p>
 * An operation span covers sending the request and receiving the response head; it is ended when
 * the operation hands its response to the caller, so it does not cover reading the response body
 * (rows are streamed by the caller afterwards).
 * <p>
 * A recorder that does not record a given kind of span returns {@link DefaultSpanRecorder#NOOP_SPAN}
 * instead, so the client never has to check for {@code null}.
 * <p>
 * A span is used by a single operation at a time, but the operation span and its request spans may
 * be touched from different threads (an operation may run on the shared operation executor), so an
 * implementation should not assume single-thread access.
 */
public interface Span {

    /**
     * Records an attribute. The keys the client uses are listed in {@link SpanAttribute}; there is a
     * single attribute method so that an implementation cannot miss a value by overriding only one
     * of several overloads.
     *
     * @param key - attribute key, see {@link SpanAttribute#getKey()}
     * @param value - attribute value; a {@code String}, {@code Number} or {@code Boolean}, never
     *              {@code null}
     */
    void setAttribute(String key, Object value);

    /**
     * Marks the span as failed and records {@link SpanAttribute#ERROR_TYPE} with the given value.
     * An implementation should map this to its own failure status - for example an OpenTelemetry
     * span status of {@code ERROR}.
     *
     * @param errorType - short, low-cardinality error identifier, usually an exception class name
     */
    void setError(String errorType);

    /**
     * Ends the span. Called exactly once by the client, also when the operation failed.
     * An implementation should be idempotent, so that ending an already-ended span is harmless.
     */
    void end();
}
