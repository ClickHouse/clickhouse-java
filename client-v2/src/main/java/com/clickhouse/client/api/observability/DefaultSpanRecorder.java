package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.query.QuerySettings;

/**
 * Base class for {@link SpanRecorder} implementations. Every method returns {@link #NOOP_SPAN}, so
 * a subclass overrides only the kinds of spans it wants to record and keeps working when the client
 * starts a kind of span the subclass does not know about.
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

    @Override
    public Span startSpan(String spanName, QuerySettings settings) {
        return NOOP_SPAN;
    }

    @Override
    public Span startSpan(String spanName, InsertSettings settings) {
        return NOOP_SPAN;
    }

    @Override
    public Span startRequestSpan(String spanName, Span operationSpan) {
        return NOOP_SPAN;
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
