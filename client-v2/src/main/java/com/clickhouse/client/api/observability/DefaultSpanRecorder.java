package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;

/**
 * Base class for {@link SpanRecorder} implementations. Every method records nothing and every
 * {@code start...} method returns {@link #NOOP_SPAN}, so a subclass overrides only what it wants to
 * record and keeps working when the client starts a kind of span the subclass does not know about.
 * <p>
 * A subclass creates its own spans and decides what to put on them. To report the client's standard
 * span names and attributes it can hand the structures it is given to {@link #getSpanSupport()}:
 * <pre>{@code
 * public Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint) {
 *     SpanSupport support = getSpanSupport();
 *     MySpan span = new MySpan(support.querySpanName(settings));
 *     support.fillQueryAttributes(span, settings, sqlQuery, endpoint);
 *     return span;
 * }
 * }</pre>
 * Using it is optional - a recorder that reports something else, or in another form, ignores it, and
 * one that wants other values overrides {@link #getSpanSupport()} with its own subclass of
 * {@link SpanSupport}.
 * <p>
 * An instance of this class itself records nothing and is what the client uses when no recorder is
 * registered.
 */
public class DefaultSpanRecorder implements SpanRecorder {

    /**
     * Span that records nothing. Returned by every method of this class and used by the client
     * whenever there is nothing to record, so that a span reference is never {@code null}.
     */
    public static final Span NOOP_SPAN = new NoopSpan();

    /**
     * Shared instance that records nothing.
     */
    public static final DefaultSpanRecorder NOOP = new DefaultSpanRecorder();

    /**
     * Returns the helper a subclass can use to derive the client's standard span names and
     * attributes. Override to report other values.
     *
     * @return span support; never {@code null}
     */
    protected SpanSupport getSpanSupport() {
        return SpanSupport.DEFAULT;
    }

    @Override
    public Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint) {
        return NOOP_SPAN;
    }

    @Override
    public Span startInsertSpan(InsertSettings settings, String tableName, int batchSize, Endpoint endpoint) {
        return NOOP_SPAN;
    }

    @Override
    public Span startRequestSpan(Span operationSpan, String host, int port) {
        return NOOP_SPAN;
    }

    @Override
    public void recordHttpStatus(Span requestSpan, int statusCode) {
        // records nothing
    }

    @Override
    public void recordQuerySuccess(Span operationSpan, OperationMetrics metrics) {
        // records nothing
    }

    @Override
    public void recordInsertSuccess(Span operationSpan, OperationMetrics metrics) {
        // records nothing
    }

    @Override
    public void recordFailure(Span operationSpan, Throwable t) {
        // records nothing
    }

    @Override
    public void recordRequestFailure(Span requestSpan, Throwable t) {
        // records nothing
    }

    /**
     * Span implementation that discards everything reported to it.
     */
    private static final class NoopSpan implements Span {

        @Override
        public void setAttribute(String key, Object value) {
            // records nothing
        }

        @Override
        public void setError(String errorType) {
            // records nothing
        }

        @Override
        public void end() {
            // records nothing
        }

        @Override
        public String toString() {
            return "NoopSpan";
        }
    }
}
