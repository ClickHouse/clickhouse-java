package com.clickhouse.client.api.observability.otel;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.observability.Span;
import com.clickhouse.client.api.observability.SpanAttribute;
import com.clickhouse.client.api.observability.SpanRecorder;
import com.clickhouse.client.api.observability.SpanSupport;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.client.api.transport.Endpoint;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SpanRecorder} that reports client operations and transport requests as OpenTelemetry spans.
 * <p>
 * It is registered like any other recorder:
 * <pre>{@code
 * Client client = new Client.Builder()
 *         .addEndpoint("http://localhost:8123")
 *         .setSpanRecorder(new OpenTelemetrySpanRecorder(openTelemetry))
 *         .build();
 * }</pre>
 * Every span is a {@link SpanKind#CLIENT} span and carries the client's standard name and
 * attributes, which are derived by {@link SpanSupport} - so the recorded keys are the ones listed in
 * {@link SpanAttribute} and mean the same as for every other recorder.
 * <p>
 * An operation span is started as a child of the {@linkplain Context#current() current context}, so
 * it appears under the application's own span when the operation is started on a thread that has
 * one. A request span is a child of the operation span it was started for. The recorder does not
 * make any span current: the client hands the response to the caller before the response body is
 * read, so a span is ended on a thread the recorder does not control.
 * <p>
 * Instances are thread-safe and can be shared by several clients.
 */
public class OpenTelemetrySpanRecorder implements SpanRecorder {

    /**
     * Default instrumentation scope name. It is reported for the spans of a recorder created by the
     * no-argument constructor or by {@link #OpenTelemetrySpanRecorder(OpenTelemetry)}. A recorder
     * created by {@link #OpenTelemetrySpanRecorder(Tracer)} reports the scope of the given tracer
     * instead.
     */
    public static final String INSTRUMENTATION_SCOPE_NAME = "com.clickhouse.client";

    private final SpanSupport spanSupport = SpanSupport.DEFAULT;

    /**
     * Tracer the spans are created with, or {@code null} when they are created with the tracer of the
     * global OpenTelemetry instance, which is then read every time a span is started.
     */
    private final Tracer tracer;

    /**
     * Creates a recorder that reports to the {@linkplain GlobalOpenTelemetry#get() global}
     * OpenTelemetry instance. Use it when the application configures OpenTelemetry globally, for
     * example through the OpenTelemetry Java agent or the autoconfigure SDK extension.
     * <p>
     * The global instance is read when a span is started, not here, so a client may be created before
     * the application installs its OpenTelemetry SDK.
     */
    public OpenTelemetrySpanRecorder() {
        this.tracer = null;
    }

    /**
     * Creates a recorder that reports to the given OpenTelemetry instance.
     *
     * @param openTelemetry - OpenTelemetry instance to report to; must not be {@code null}
     */
    public OpenTelemetrySpanRecorder(OpenTelemetry openTelemetry) {
        this(tracerOf(openTelemetry));
    }

    /**
     * Creates a recorder that reports to the given tracer. Use it to report the client's spans under
     * an instrumentation scope of the application's choice.
     *
     * @param tracer - tracer to create spans with; must not be {@code null}
     */
    public OpenTelemetrySpanRecorder(Tracer tracer) {
        if (tracer == null) {
            throw new IllegalArgumentException("tracer must not be null");
        }
        this.tracer = tracer;
    }

    private static Tracer tracerOf(OpenTelemetry openTelemetry) {
        if (openTelemetry == null) {
            throw new IllegalArgumentException("openTelemetry must not be null");
        }
        return openTelemetry.getTracer(INSTRUMENTATION_SCOPE_NAME);
    }

    /**
     * Returns the tracer the next span is created with - the one given to this recorder, or the tracer
     * of the global OpenTelemetry instance as it is installed now.
     *
     * @return tracer; never {@code null}
     */
    protected Tracer getTracer() {
        return tracer != null ? tracer : GlobalOpenTelemetry.get().getTracer(INSTRUMENTATION_SCOPE_NAME);
    }

    @Override
    public Span startQuerySpan(QuerySettings settings, String sqlQuery, Endpoint endpoint) {
        OpenTelemetrySpan span = startSpan(spanSupport.querySpanName(settings), Context.current());
        spanSupport.fillQueryAttributes(span, settings, sqlQuery, endpoint);
        return span;
    }

    @Override
    public Span startInsertSpan(InsertSettings settings, String tableName, int batchSize, Endpoint endpoint) {
        OpenTelemetrySpan span = startSpan(spanSupport.insertSpanName(settings, tableName), Context.current());
        spanSupport.fillInsertAttributes(span, settings, tableName, batchSize, endpoint);
        return span;
    }

    @Override
    public Span startRequestSpan(Span operationSpan, String host, int port) {
        OpenTelemetrySpan span = startSpan(spanSupport.requestSpanName(), parentContextOf(operationSpan));
        spanSupport.fillRequestAttributes(span, host, port);
        return span;
    }

    @Override
    public void recordHttpStatus(Span requestSpan, int statusCode) {
        spanSupport.recordHttpStatus(requestSpan, statusCode);
    }

    @Override
    public void recordQuerySuccess(Span operationSpan, OperationMetrics metrics) {
        spanSupport.recordQuerySuccess(operationSpan, metrics);
    }

    @Override
    public void recordInsertSuccess(Span operationSpan, OperationMetrics metrics) {
        spanSupport.recordInsertSuccess(operationSpan, metrics);
    }

    @Override
    public void recordFailure(Span operationSpan, Throwable t) {
        spanSupport.recordFailure(operationSpan, t);
        recordException(operationSpan, t);
    }

    @Override
    public void recordRequestFailure(Span requestSpan, Throwable t) {
        spanSupport.recordRequestFailure(requestSpan, t);
        recordException(requestSpan, t);
    }

    /**
     * Records the failure itself as an OpenTelemetry exception event, so that its message and stack
     * trace are reported next to the {@link SpanAttribute#ERROR_TYPE} attribute.
     *
     * @param span - span the failure was reported on
     * @param t - failure, may be {@code null}
     */
    protected void recordException(Span span, Throwable t) {
        if (t != null && span instanceof OpenTelemetrySpan) {
            ((OpenTelemetrySpan) span).getSpan().recordException(t);
        }
    }

    /**
     * Starts a client span with the given name under the given parent context.
     *
     * @param spanName - name of the span
     * @param parentContext - context the span is started under
     * @return new span
     */
    protected OpenTelemetrySpan startSpan(String spanName, Context parentContext) {
        io.opentelemetry.api.trace.Span span = getTracer().spanBuilder(spanName)
                .setSpanKind(SpanKind.CLIENT)
                .setParent(parentContext)
                .startSpan();
        return new OpenTelemetrySpan(span, parentContext.with(span));
    }

    /**
     * Returns the context a request span is started under - the context of its operation span, or the
     * current context when the operation span was not created by this recorder.
     */
    private static Context parentContextOf(Span operationSpan) {
        return operationSpan instanceof OpenTelemetrySpan
                ? ((OpenTelemetrySpan) operationSpan).getContext()
                : Context.current();
    }

    /**
     * {@link Span} backed by an OpenTelemetry span.
     */
    public static class OpenTelemetrySpan implements Span {

        private final io.opentelemetry.api.trace.Span span;

        private final Context context;

        private final AtomicBoolean ended = new AtomicBoolean();

        OpenTelemetrySpan(io.opentelemetry.api.trace.Span span, Context context) {
            this.span = span;
            this.context = context;
        }

        /**
         * Returns the OpenTelemetry span this span records on.
         *
         * @return OpenTelemetry span
         */
        public io.opentelemetry.api.trace.Span getSpan() {
            return span;
        }

        /**
         * Returns the context that holds this span. It is the parent context of the spans started for
         * the same operation.
         *
         * @return context holding this span
         */
        public Context getContext() {
            return context;
        }

        @Override
        public void setAttribute(String key, Object value) {
            if (key == null || value == null) {
                return;
            }
            if (value instanceof String) {
                span.setAttribute(AttributeKey.stringKey(key), (String) value);
            } else if (value instanceof Boolean) {
                span.setAttribute(AttributeKey.booleanKey(key), (Boolean) value);
            } else if (value instanceof Double || value instanceof Float) {
                span.setAttribute(AttributeKey.doubleKey(key), ((Number) value).doubleValue());
            } else if (value instanceof Number) {
                span.setAttribute(AttributeKey.longKey(key), ((Number) value).longValue());
            } else {
                span.setAttribute(AttributeKey.stringKey(key), String.valueOf(value));
            }
        }

        @Override
        public void setError(String errorType) {
            span.setStatus(StatusCode.ERROR);
            if (errorType != null) {
                span.setAttribute(AttributeKey.stringKey(SpanAttribute.ERROR_TYPE.getKey()), errorType);
            }
        }

        @Override
        public void end() {
            if (ended.compareAndSet(false, true)) {
                span.end();
            }
        }

        @Override
        public String toString() {
            return "OpenTelemetrySpan[" + span.getSpanContext().getSpanId() + "]";
        }
    }
}
