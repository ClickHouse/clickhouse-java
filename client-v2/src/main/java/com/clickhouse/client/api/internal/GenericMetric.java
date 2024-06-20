package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.metrics.Metric;

public class GenericMetric implements Metric {

    private volatile Object value;

    public GenericMetric(Object value) {
        this.value = value;
    }

    public void set(Object value) {
        this.value = value;
    }

    @Override
    public long getLong() {
        return Long.parseLong(getString());
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }
}
