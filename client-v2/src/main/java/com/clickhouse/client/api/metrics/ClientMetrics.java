package com.clickhouse.client.api.metrics;

public enum ClientMetrics {

    /**
     * Operation duration in milliseconds.
     */
    OP_DURATION("client.opDuration"),

    /**
     * Duration of the operation serialization step in milliseconds.
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
