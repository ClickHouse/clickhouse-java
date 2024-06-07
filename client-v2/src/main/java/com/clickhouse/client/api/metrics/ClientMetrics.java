package com.clickhouse.client.api.metrics;

public enum ClientMetrics {

    /**
     * Operation duration in nanoseconds.
     */
    OP_DURATION("client.opDuration"),

    /**
     * Duration of the operation serialization step in nanoseconds.
     */
    OP_SERIALIZATION("client.opSerialization");

    private final String key;

    ClientMetrics(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
