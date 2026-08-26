package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.QuerySettings;

import java.time.Duration;

/**
 * Base class for {@link MetricsRecorder} implementations. Every method records nothing, so a
 * subclass overrides only what it wants to record and keeps working when the client starts reporting
 * an event the subclass does not know about.
 * <p>
 * A subclass owns its instruments and decides what to report. To use the client's standard metric
 * names, units and attributes it can hand the structures it is given to {@link #getMetricsSupport()}:
 * <pre>{@code
 * public void recordQuerySuccess(QuerySettings settings, OperationMetrics metrics) {
 *     MetricsSupport support = getMetricsSupport();
 *     myHistogram.record(support.operationDuration(metrics), support.queryAttributes(settings, null));
 * }
 * }</pre>
 * Using it is optional - a recorder that reports something else, or in another form, ignores it, and
 * one that wants other values overrides {@link #getMetricsSupport()} with its own subclass of
 * {@link MetricsSupport}.
 * <p>
 * An instance of this class itself records nothing and is what the client uses when no recorder is
 * registered.
 */
public class DefaultMetricsRecorder implements MetricsRecorder {

    /**
     * Shared instance that records nothing.
     */
    public static final DefaultMetricsRecorder NOOP = new DefaultMetricsRecorder();

    /**
     * Returns the helper a subclass can use to derive the client's standard metric names, units and
     * attributes. Override to report other values.
     *
     * @return metrics support; never {@code null}
     */
    protected MetricsSupport getMetricsSupport() {
        return MetricsSupport.DEFAULT;
    }

    @Override
    public void recordQuerySuccess(QuerySettings settings, OperationMetrics metrics) {
        // records nothing
    }

    @Override
    public void recordInsertSuccess(InsertSettings settings, String tableName, OperationMetrics metrics) {
        // records nothing
    }

    @Override
    public void recordQueryFailure(QuerySettings settings, Duration duration, Throwable t) {
        // records nothing
    }

    @Override
    public void recordInsertFailure(InsertSettings settings, String tableName, Duration duration, Throwable t) {
        // records nothing
    }

    @Override
    public void recordQueryRetry(QuerySettings settings, Throwable cause) {
        // records nothing
    }

    @Override
    public void recordInsertRetry(InsertSettings settings, String tableName, Throwable cause) {
        // records nothing
    }
}
