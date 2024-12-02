package com.clickhouse.client.api.metrics;

import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.internal.Gauge;
import com.clickhouse.client.api.internal.StopWatch;

import java.util.HashMap;
import java.util.Map;

/**
 * OperationStatistics objects hold various stats for complete operations.
 * <p>
 * It can be used for logging or monitoring purposes.
 */
public class OperationMetrics {

    public Map<String, Metric> metrics = new HashMap<>();
    private String queryId;

    private final ClientStatisticsHolder clientStatistics;

    public OperationMetrics(ClientStatisticsHolder clientStatisticsHolder) {
        this.clientStatistics = clientStatisticsHolder;
    }

    public Metric getMetric(ServerMetrics metric) {
        return metrics.get(metric.getKey());
    }

    public Metric getMetric(ClientMetrics metric) {
        return metrics.get(metric.getKey());
    }

    public String getQueryId() {
        return queryId;
    }

    /**
     * Complete counting metrics on operation and stop all stopwatches.
     * Multiple calls may have side effects.
     * Note: should not be called by user code, except when created by user code.
     */
    public void operationComplete() {
        for (Map.Entry<String, StopWatch> sw : clientStatistics.getStopWatches().entrySet()) {
            sw.getValue().stop();
            metrics.put(sw.getKey(), sw.getValue());
        }
    }

    public void updateMetric(ServerMetrics metric, long value) {
        metrics.put(metric.getKey(), new Gauge(value));
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    @Override
    public String toString() {
        return "OperationStatistics{" +
                "\"queryId\"=\"" + queryId + "\", " +
                "\"metrics\"=" + metrics +
                '}';
    }
}
