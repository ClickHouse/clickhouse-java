package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.metrics.ClientMetrics;

import java.util.HashMap;
import java.util.Map;

public class ClientStatisticsHolder {

    private final Map<String, StopWatch> stopWatches = new HashMap<>();

    public void start(ClientMetrics metric) {
        start(metric.getKey());
    }

    public void start(String spanName) {
        stopWatches.computeIfAbsent(spanName, k -> new StopWatch()).start();
    }

    public StopWatch stop(ClientMetrics metric) {
        return stop(metric.getKey());
    }

    public StopWatch stop(String spanName) {
        StopWatch timer = stopWatches.computeIfAbsent(spanName, k -> new StopWatch());
        timer.stop();
        return timer;
    }

    public long getElapsedTime(String spanName) {
        StopWatch sw = stopWatches.get(spanName);
        return sw == null ? -1 : sw.getElapsedTime();
    }

    public Map<String, StopWatch> getStopWatches() {
        return stopWatches;
    }

    @Override
    public String toString() {
        return "ClientStatistics{" +
                "\"spans\"=" + stopWatches +
                '}';
    }
}
