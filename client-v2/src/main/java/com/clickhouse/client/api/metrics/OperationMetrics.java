package com.clickhouse.client.api.metrics;

import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.api.internal.ClientStatisticsHolder;
import com.clickhouse.client.api.internal.Gauge;
import com.clickhouse.client.api.internal.GenericMetric;
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

    public void operationComplete(ClickHouseResponseSummary serverStats) {
        for (Map.Entry<String, StopWatch> sw : clientStatistics.getStopWatches().entrySet()) {
            sw.getValue().stop();
            metrics.put(sw.getKey(), sw.getValue());
        }
        metrics.put(ServerMetrics.NUM_ROWS_READ.getKey(), new Gauge(serverStats.getReadRows()));
        metrics.put(ServerMetrics.NUM_ROWS_WRITTEN.getKey(), new Gauge(serverStats.getWrittenRows()));
        metrics.put(ServerMetrics.TOTAL_ROWS_TO_READ.getKey(), new Gauge(serverStats.getTotalRowsToRead()));
        metrics.put(ServerMetrics.NUM_BYTES_READ.getKey(), new Gauge(serverStats.getReadBytes()));
        metrics.put(ServerMetrics.NUM_BYTES_WRITTEN.getKey(), new Gauge(serverStats.getWrittenBytes()));
        metrics.put(ServerMetrics.RESULT_ROWS.getKey(), new Gauge(serverStats.getResultRows()));
        metrics.put(ServerMetrics.ELAPSED_TIME.getKey(), new Gauge(serverStats.getElapsedTime()));
        metrics.put(ServerMetrics.QUERY_ID.getKey(), new GenericMetric(serverStats.getQueryId()));
    }

    @Override
    public String toString() {
        return "OperationStatistics{" +
                "\"metrics\"=" + metrics +
                '}';
    }
}
