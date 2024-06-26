package com.clickhouse.client.api.metrics;

public interface Metric {

    /**
     * Returns value of the metric as a long.
     * @return value of the metric as a long
     */
    long getLong();

}
