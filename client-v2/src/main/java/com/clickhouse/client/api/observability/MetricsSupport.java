package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.ServerException;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.internal.StopWatch;
import com.clickhouse.client.api.metrics.ClientMetrics;
import com.clickhouse.client.api.metrics.Metric;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.QuerySettings;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Derives the client's standard metric attributes and values from the structures a
 * {@link MetricsRecorder} is called with.
 * <p>
 * This class is a helper <b>for</b> a recorder, not a layer in front of one: the client always calls
 * the registered recorder first, and an implementation decides whether to use this class. Using it is
 * how a recorder reports the same values under the same {@link MetricName} and the same
 * {@link MetricAttribute} keys as every other recorder; a recorder that wants other values either
 * overrides the method that computes them, or does not use this class at all.
 * <p>
 * Every method may be overridden. {@link #DEFAULT} is a shared instance for implementations that keep
 * the standard behaviour - the class holds no state.
 */
public class MetricsSupport {

    /**
     * Shared instance with the standard behaviour.
     */
    public static final MetricsSupport DEFAULT = new MetricsSupport();

    /**
     * Value of {@link MetricAttribute#DB_SYSTEM_NAME}.
     */
    public static final String DB_SYSTEM_NAME = "clickhouse";

    public static final String OPERATION_QUERY = "query";

    public static final String OPERATION_INSERT = "insert";

    /**
     * Returned by the duration methods when the client did not measure that duration, so that a
     * recorder does not report a made-up value.
     */
    public static final double DURATION_UNKNOWN = -1d;

    private static final double NANOS_PER_SECOND = 1_000_000_000d;

    private static final double MILLIS_PER_SECOND = 1_000d;

    /**
     * Returns the attributes of a read operation - a query, a command, a ping or a table-schema
     * lookup.
     *
     * @param settings - resolved settings of the operation
     * @param failure - failure of the operation, or {@code null} when it succeeded
     * @return attributes, keyed by {@link MetricAttribute#getKey()}
     */
    public Map<String, Object> queryAttributes(QuerySettings settings, Throwable failure) {
        return attributes(settings == null ? null : settings.getDatabase(), OPERATION_QUERY, null, failure);
    }

    /**
     * Returns the attributes of an insert operation.
     *
     * @param settings - resolved settings of the operation
     * @param tableName - target table
     * @param failure - failure of the operation, or {@code null} when it succeeded
     * @return attributes, keyed by {@link MetricAttribute#getKey()}
     */
    public Map<String, Object> insertAttributes(InsertSettings settings, String tableName, Throwable failure) {
        return attributes(settings == null ? null : settings.getDatabase(), OPERATION_INSERT, tableName, failure);
    }

    /**
     * Returns the duration of a completed operation in seconds, the unit of
     * {@link MetricName#OPERATION_DURATION}.
     *
     * @param metrics - metrics of the completed operation, may be {@code null}
     * @return duration in seconds, or {@link #DURATION_UNKNOWN} when the client did not measure it
     */
    public double operationDuration(OperationMetrics metrics) {
        return durationOf(metrics, ClientMetrics.OP_DURATION);
    }

    /**
     * Returns the duration of the serialization step of a completed operation in seconds, the unit of
     * {@link MetricName#OPERATION_SERIALIZATION_DURATION}.
     *
     * @param metrics - metrics of the completed operation, may be {@code null}
     * @return duration in seconds, or {@link #DURATION_UNKNOWN} when the client did not measure it
     */
    public double serializationDuration(OperationMetrics metrics) {
        return durationOf(metrics, ClientMetrics.OP_SERIALIZATION);
    }

    /**
     * Returns a duration the client measured itself - the duration of a failed operation - in
     * seconds, the unit of {@link MetricName#OPERATION_DURATION}.
     *
     * @param duration - measured duration, may be {@code null}
     * @return duration in seconds, or {@link #DURATION_UNKNOWN} when there is none
     */
    public double duration(Duration duration) {
        return duration == null ? DURATION_UNKNOWN : duration.toNanos() / NANOS_PER_SECOND;
    }

    /**
     * Returns the value of {@link MetricAttribute#ERROR_TYPE} for a failure - a short,
     * low-cardinality identifier of what went wrong. A failure the server reported is identified by
     * the server error it carries, the same way {@link SpanSupport} identifies it, so a span and a
     * metric of the same failure report the same error type.
     *
     * @param t - failure, may be {@code null}
     * @return error type, or {@code null} when there is no failure
     */
    public String errorType(Throwable t) {
        if (t == null) {
            return null;
        }

        ServerException serverException = findServerException(t);
        return serverException == null ? t.getClass().getName() : serverException.getClass().getName();
    }

    /**
     * Reads one duration of a completed operation and converts it to seconds. A duration the client
     * did not measure is not reported.
     *
     * @param metrics - metrics of the completed operation, may be {@code null}
     * @param metric - duration to read
     * @return duration in seconds, or {@link #DURATION_UNKNOWN}
     */
    protected double durationOf(OperationMetrics metrics, ClientMetrics metric) {
        if (metrics == null) {
            return DURATION_UNKNOWN;
        }

        Metric value = metrics.getMetric(metric);
        if (value == null) {
            return DURATION_UNKNOWN;
        }
        // A stopwatch keeps nanoseconds, while the Metric contract exposes whole milliseconds, which
        // would round a fast operation down to zero.
        if (value instanceof StopWatch) {
            return ((StopWatch) value).getElapsedNanos() / NANOS_PER_SECOND;
        }
        return value.getLong() / MILLIS_PER_SECOND;
    }

    /**
     * Returns the attributes that describe an operation. A value the client does not know is left
     * out, so a recorder never reports an attribute with a placeholder value.
     */
    protected Map<String, Object> attributes(String namespace, String operationName, String collectionName,
                                             Throwable failure) {
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put(MetricAttribute.DB_SYSTEM_NAME.getKey(), DB_SYSTEM_NAME);
        if (namespace != null) {
            attributes.put(MetricAttribute.DB_NAMESPACE.getKey(), namespace);
        }
        if (operationName != null) {
            attributes.put(MetricAttribute.DB_OPERATION_NAME.getKey(), operationName);
        }
        if (collectionName != null) {
            attributes.put(MetricAttribute.DB_COLLECTION_NAME.getKey(), collectionName);
        }
        if (failure != null) {
            ServerException serverException = findServerException(failure);
            if (serverException != null) {
                attributes.put(MetricAttribute.DB_RESPONSE_STATUS_CODE.getKey(), serverException.getCode());
            }
            attributes.put(MetricAttribute.ERROR_TYPE.getKey(), errorType(failure));
        }
        return Collections.unmodifiableMap(attributes);
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
}
