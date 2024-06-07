package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.metrics.Metric;

public class Gauge implements Metric {

    private volatile long value;

    public Gauge(long readRows) {
        this.value = readRows;
    }

    public void set(long value) {
        this.value = value;
    }

    @Override
    public long getLong() {
        return value;
    }
}
