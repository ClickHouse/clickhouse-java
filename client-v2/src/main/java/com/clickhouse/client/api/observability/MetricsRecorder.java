package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.QuerySettings;

import java.time.Duration;

/**
 * Backend-agnostic hook that lets an application export the metrics of client operations.
 * <p>
 * A recorder is registered with
 * {@link com.clickhouse.client.api.Client.Builder#setMetricsRecorder(MetricsRecorder)}. It is called
 * once per completed operation, with everything the client knows about it, so an implementation is
 * free to report whatever it needs and however it wants - including nothing.
 * <p>
 * Three kinds of events are reported:
 * <ul>
 *     <li><b>Success</b> - {@link #recordQuerySuccess(QuerySettings, OperationMetrics)} for a read
 *     operation and {@link #recordInsertSuccess(InsertSettings, String, OperationMetrics)} for an
 *     insert. The durations of the operation are read from the metrics of the completed
 *     operation.</li>
 *     <li><b>Failure</b> - {@link #recordQueryFailure(QuerySettings, Duration, Throwable)} and
 *     {@link #recordInsertFailure(InsertSettings, String, Duration, Throwable)}. An operation that
 *     failed has no metrics, so the client measures its duration itself and passes it in. It measures
 *     the same work a successful operation reports - from where it starts the operation until the
 *     outcome is known, before any recorder runs - so that both outcomes form one latency series.</li>
 *     <li><b>Retry</b> - {@link #recordQueryRetry(QuerySettings, Throwable)} and
 *     {@link #recordInsertRetry(InsertSettings, String, Throwable)}, called once per retried
 *     attempt. The operation itself may still succeed.</li>
 * </ul>
 * Exactly one success or one failure event is reported per operation the client started, so counting
 * those events gives the number of operations by outcome.
 * <p>
 * An implementation does not have to derive the standard metric names, units and attributes itself:
 * it may call {@link MetricsSupport}, which computes them - the names listed in {@link MetricName}
 * and the keys listed in {@link MetricAttribute} - from the same structures. That is opt-in; a
 * recorder that wants to report something else, or in another form, simply does not use it.
 * <p>
 * Implementations should extend {@link DefaultMetricsRecorder} and override only what they care
 * about; the inherited methods record nothing, so a recorder keeps working when the client starts
 * reporting an event it does not know about.
 * <p>
 * A recorder is shared by all operations of a client instance and must be thread-safe. It is called on
 * the thread that runs the operation and must not throw and not block, because a failure of a recorder
 * is a failure of the operation it reports.
 */
public interface MetricsRecorder {

    /**
     * Reports that a read operation - a query, a command, a ping or a table-schema lookup -
     * completed successfully.
     *
     * @param settings - resolved settings of the operation; source of the target database
     * @param metrics - metrics of the completed operation; source of the operation duration and of
     *                what the server read and returned. May be {@code null}
     */
    void recordQuerySuccess(QuerySettings settings, OperationMetrics metrics);

    /**
     * Reports that an insert operation completed successfully.
     *
     * @param settings - resolved settings of the operation; source of the target database
     * @param tableName - target table
     * @param metrics - metrics of the completed operation; source of the operation and serialization
     *                durations and of what the server wrote. May be {@code null}
     */
    void recordInsertSuccess(InsertSettings settings, String tableName, OperationMetrics metrics);

    /**
     * Reports that a read operation failed. It is the counterpart of
     * {@link #recordQuerySuccess(QuerySettings, OperationMetrics)}.
     *
     * @param settings - resolved settings of the operation
     * @param duration - time the operation took before it failed, measured by the client
     * @param t - failure the caller receives
     */
    void recordQueryFailure(QuerySettings settings, Duration duration, Throwable t);

    /**
     * Reports that an insert operation failed. It is the counterpart of
     * {@link #recordInsertSuccess(InsertSettings, String, OperationMetrics)}.
     *
     * @param settings - resolved settings of the operation
     * @param tableName - target table
     * @param duration - time the operation took before it failed, measured by the client
     * @param t - failure the caller receives
     */
    void recordInsertFailure(InsertSettings settings, String tableName, Duration duration, Throwable t);

    /**
     * Reports that an attempt of a read operation failed and the client retries it. Called once per
     * retried attempt, so an operation that succeeds on its third attempt reports two retries.
     *
     * @param settings - resolved settings of the operation
     * @param cause - failure of the attempt that is retried, reported the way the operation would
     *              report it if no further attempt succeeded
     */
    void recordQueryRetry(QuerySettings settings, Throwable cause);

    /**
     * Reports that an attempt of an insert operation failed and the client retries it. Called once
     * per retried attempt.
     *
     * @param settings - resolved settings of the operation
     * @param tableName - target table
     * @param cause - failure of the attempt that is retried
     */
    void recordInsertRetry(InsertSettings settings, String tableName, Throwable cause);
}
