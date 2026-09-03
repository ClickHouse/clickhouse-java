package com.clickhouse.client.api.observability;

import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.client.api.metrics.OperationMetrics;
import com.clickhouse.client.api.query.QuerySettings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Recorder that keeps every metric it reports, so tests can assert what the client reported. It takes
 * the values and the attributes from {@link MetricsSupport}, which is how a recorder opts in to the
 * client's standard values.
 */
public class CapturingMetricsRecorder extends DefaultMetricsRecorder {

    private final List<RecordedMetric> metrics = Collections.synchronizedList(new ArrayList<>());

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
        add(MetricName.OPERATION_RETRIES, 1, getMetricsSupport().queryAttributes(settings, cause));
    }

    @Override
    public void recordInsertRetry(InsertSettings settings, String tableName, Throwable cause) {
        add(MetricName.OPERATION_RETRIES, 1, getMetricsSupport().insertAttributes(settings, tableName, cause));
    }

    private void recordCompletion(double duration, double serializationDuration, Map<String, Object> attributes) {
        add(MetricName.OPERATION_COUNT, 1, attributes);
        add(MetricName.OPERATION_DURATION, duration, attributes);
        if (serializationDuration != MetricsSupport.DURATION_UNKNOWN) {
            add(MetricName.OPERATION_SERIALIZATION_DURATION, serializationDuration, attributes);
        }
    }

    private void add(MetricName name, double value, Map<String, Object> attributes) {
        metrics.add(new RecordedMetric(name, value, attributes));
    }

    public List<RecordedMetric> getMetrics() {
        synchronized (metrics) {
            return new ArrayList<>(metrics);
        }
    }

    /**
     * Returns the values reported for the given metric, in the order they were reported.
     */
    public List<RecordedMetric> getMetrics(MetricName name) {
        List<RecordedMetric> selected = new ArrayList<>();
        for (RecordedMetric metric : getMetrics()) {
            if (metric.getName() == name) {
                selected.add(metric);
            }
        }
        return selected;
    }

    /**
     * Returns the only value reported for the given metric.
     */
    public RecordedMetric getOnlyMetric(MetricName name) {
        List<RecordedMetric> selected = getMetrics(name);
        if (selected.size() != 1) {
            throw new AssertionError("Expected exactly one " + name + " but got " + selected);
        }
        return selected.get(0);
    }

    public void clear() {
        metrics.clear();
    }

    public static final class RecordedMetric {

        private final MetricName name;
        private final double value;
        private final Map<String, Object> attributes;

        RecordedMetric(MetricName name, double value, Map<String, Object> attributes) {
            this.name = name;
            this.value = value;
            this.attributes = new LinkedHashMap<>(attributes);
        }

        public MetricName getName() {
            return name;
        }

        public double getValue() {
            return value;
        }

        public Map<String, Object> getAttributes() {
            return Collections.unmodifiableMap(attributes);
        }

        public Object getAttribute(MetricAttribute attribute) {
            return attributes.get(attribute.getKey());
        }

        @Override
        public String toString() {
            return "RecordedMetric{name=" + name + ", value=" + value + ", attributes=" + attributes + '}';
        }
    }
}
