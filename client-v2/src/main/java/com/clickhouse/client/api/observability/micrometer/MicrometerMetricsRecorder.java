package com.clickhouse.client.api.observability.micrometer;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.observability.DefaultMetricsRecorder;
import com.clickhouse.client.api.observability.MetricAttribute;
import com.clickhouse.client.api.observability.MetricName;
import com.clickhouse.client.api.observability.MetricsRecorder;
import com.clickhouse.client.api.observability.MetricsSupport;
import com.clickhouse.client.api.query.QuerySettings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link MetricsRecorder} that reports the metrics of client operations to a Micrometer
 * {@link MeterRegistry}.
 * <p>
 * It is registered like any other recorder:
 * <pre>{@code
 * Client client = new Client.Builder()
 *         .addEndpoint("http://localhost:8123")
 *         .setMetricsRecorder(new MicrometerMetricsRecorder(meterRegistry))
 *         .build();
 * }</pre>
 * The no-argument constructor reports to {@link Metrics#globalRegistry}, so the recorder can also be
 * named by the jdbc-v2 {@code jdbc_metrics_recorder} connection property, which instantiates the
 * class it names through its public no-argument constructor.
 * <p>
 * Every meter carries the client's standard name, description and unit, which are the ones of
 * {@link MetricName}, and the recorded values and attributes are derived by {@link MetricsSupport},
 * so the tag keys are the ones listed in {@link MetricAttribute} and both mean the same as for every
 * other recorder. Four meters are reported:
 * <ul>
 *     <li>a {@link Timer} named {@link MetricName#OPERATION_DURATION} per completed operation,
 *     successful or failed;</li>
 *     <li>a {@link Timer} named {@link MetricName#OPERATION_SERIALIZATION_DURATION} when the client
 *     measured the serialization step, which it does for an insert of POJOs;</li>
 *     <li>a {@link Counter} named {@link MetricName#OPERATION_COUNT} per completed operation;</li>
 *     <li>a {@link Counter} named {@link MetricName#OPERATION_RETRIES} per retried attempt.</li>
 * </ul>
 * Every meter of a metric carries the same tag keys - all keys of {@link MetricAttribute} - and the
 * value of an attribute the client did not report is {@link #ABSENT_ATTRIBUTE_VALUE}. A tag of a
 * Micrometer meter is a label of the exported time series, and a label that appears on one outcome
 * only makes the series of a metric hard to aggregate and is rejected outright by some registries, so
 * the recorder reports the same labels for every outcome instead of leaving a tag out. A successful
 * operation therefore reports no error type while a failed one does, and the two outcomes are
 * separate time series of the same meter.
 * A duration the client did not measure is not reported at all, so a recorded duration is always one
 * the client observed.
 * <p>
 * A Micrometer timer keeps its own time unit, so the seconds of the SPI are handed to the registry as
 * nanoseconds and the registry publishes them in the unit its backend expects. A timer records a
 * count, a sum and a maximum; percentiles and a histogram are a decision of the application, which
 * enables them for these meters with a Micrometer {@code MeterFilter}.
 * <p>
 * Instances are thread-safe and can be shared by several clients.
 */
public class MicrometerMetricsRecorder extends DefaultMetricsRecorder {

    /**
     * Value of the tag of an attribute the client did not report. Every meter of a metric carries every
     * tag key, so an absent value is reported as this placeholder instead of the tag being left out.
     */
    public static final String ABSENT_ATTRIBUTE_VALUE = "none";

    private static final double NANOS_PER_SECOND = 1_000_000_000d;

    private final MeterRegistry registry;

    /**
     * Creates a recorder that reports to the {@linkplain Metrics#globalRegistry global} Micrometer
     * registry. Use it when the application configures Micrometer globally, and when the recorder is
     * named by the jdbc-v2 {@code jdbc_metrics_recorder} property, which needs a no-argument
     * constructor.
     */
    public MicrometerMetricsRecorder() {
        this(Metrics.globalRegistry);
    }

    /**
     * Creates a recorder that reports to the given registry.
     *
     * @param registry - registry the meters are registered with; must not be {@code null}
     */
    public MicrometerMetricsRecorder(MeterRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("registry must not be null");
        }
        this.registry = registry;
    }

    /**
     * Returns the registry this recorder reports to.
     *
     * @return meter registry; never {@code null}
     */
    public MeterRegistry getRegistry() {
        return registry;
    }

    @Override
    public void recordQuerySuccess(QuerySettings settings, OperationMetrics metrics) {
        MetricsSupport support = getMetricsSupport();
        recordCompletion(support.operationDuration(metrics), support.serializationDuration(metrics),
                support.queryAttributes(settings, null));
    }

    @Override
    public void recordInsertSuccess(InsertSettings settings, String tableName, OperationMetrics metrics) {
        MetricsSupport support = getMetricsSupport();
        recordCompletion(support.operationDuration(metrics), support.serializationDuration(metrics),
                support.insertAttributes(settings, tableName, null));
    }

    @Override
    public void recordQueryFailure(QuerySettings settings, Duration duration, Throwable t) {
        MetricsSupport support = getMetricsSupport();
        recordCompletion(support.duration(duration), MetricsSupport.DURATION_UNKNOWN,
                support.queryAttributes(settings, t));
    }

    @Override
    public void recordInsertFailure(InsertSettings settings, String tableName, Duration duration, Throwable t) {
        MetricsSupport support = getMetricsSupport();
        recordCompletion(support.duration(duration), MetricsSupport.DURATION_UNKNOWN,
                support.insertAttributes(settings, tableName, t));
    }

    @Override
    public void recordQueryRetry(QuerySettings settings, Throwable cause) {
        count(MetricName.OPERATION_RETRIES, tagsOf(getMetricsSupport().queryAttributes(settings, cause)));
    }

    @Override
    public void recordInsertRetry(InsertSettings settings, String tableName, Throwable cause) {
        count(MetricName.OPERATION_RETRIES,
                tagsOf(getMetricsSupport().insertAttributes(settings, tableName, cause)));
    }

    /**
     * Reports the meters of one completed operation - the operation is counted, and each duration the
     * client measured is recorded.
     *
     * @param duration - duration of the operation in seconds, or {@link MetricsSupport#DURATION_UNKNOWN}
     * @param serializationDuration - duration of the serialization step in seconds, or
     *                              {@link MetricsSupport#DURATION_UNKNOWN}
     * @param attributes - attributes of the operation, keyed by {@link MetricAttribute#getKey()}
     */
    protected void recordCompletion(double duration, double serializationDuration, Map<String, Object> attributes) {
        Tags tags = tagsOf(attributes);
        count(MetricName.OPERATION_COUNT, tags);
        recordDuration(MetricName.OPERATION_DURATION, duration, tags);
        recordDuration(MetricName.OPERATION_SERIALIZATION_DURATION, serializationDuration, tags);
    }

    /**
     * Records a duration on the timer of the given metric. A duration the client did not measure is
     * not reported, so the timer counts only the operations it has a duration of.
     *
     * @param name - metric to record
     * @param seconds - duration in seconds, or {@link MetricsSupport#DURATION_UNKNOWN}
     * @param tags - tags of the meter
     */
    protected void recordDuration(MetricName name, double seconds, Tags tags) {
        if (Double.isNaN(seconds) || seconds < 0) {
            return;
        }
        Timer.builder(name.getKey())
                .description(name.getDescription())
                .tags(tags)
                .register(registry)
                .record((long) (seconds * NANOS_PER_SECOND), TimeUnit.NANOSECONDS);
    }

    /**
     * Increments the counter of the given metric by one.
     *
     * @param name - metric to count
     * @param tags - tags of the meter
     */
    protected void count(MetricName name, Tags tags) {
        Counter.builder(name.getKey())
                .description(name.getDescription())
                .baseUnit(baseUnitOf(name))
                .tags(tags)
                .register(registry)
                .increment();
    }

    /**
     * Returns the base unit a meter of the given metric is registered with. A Micrometer registry
     * reports the base unit as a part of the name of the meter, so a unit that is a UCUM annotation
     * like {@code {operation}} - which names what is counted rather than a unit of measure - is not
     * reported, and the metric keeps the name of {@link MetricName}.
     *
     * @param name - metric a meter is registered for
     * @return base unit of the meter, or {@code null} when it has none
     */
    protected String baseUnitOf(MetricName name) {
        String unit = name.getUnit();
        return unit != null && unit.startsWith("{") ? null : unit;
    }

    /**
     * Converts the attributes of an operation to Micrometer tags. Every key of
     * {@link MetricAttribute} is reported, because a registry may require that every meter of a metric
     * carries the same tag keys; the value of an attribute the client did not report is
     * {@link #ABSENT_ATTRIBUTE_VALUE}.
     *
     * @param attributes - attributes of the operation, keyed by {@link MetricAttribute#getKey()}
     * @return tags of the meter
     */
    protected Tags tagsOf(Map<String, Object> attributes) {
        MetricAttribute[] keys = MetricAttribute.values();
        List<Tag> tags = new ArrayList<>(keys.length);
        for (MetricAttribute key : keys) {
            Object value = attributes == null ? null : attributes.get(key.getKey());
            tags.add(Tag.of(key.getKey(), value == null ? ABSENT_ATTRIBUTE_VALUE : String.valueOf(value)));
        }
        return Tags.of(tags);
    }
}
