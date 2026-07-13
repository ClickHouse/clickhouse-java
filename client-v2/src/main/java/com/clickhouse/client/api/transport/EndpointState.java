package com.clickhouse.client.api.transport;

/**
 *{@link Endpoint} is wrapped to track for failover.
 * When a node fails, it can be quarantined for a fixed duration and after
 * it is expired, it will be considered as alive again.
 */
class EndpointState {

    private final Endpoint endpoint;

    private volatile long failedUntil;

    EndpointState(Endpoint endpoint) {
        this.endpoint = endpoint;
        this.failedUntil = 0;
    }

    Endpoint getEndpoint() {
        return endpoint;
    }

    void markFailed(long quarantineMs) {
        if (quarantineMs <= 0) {
            throw new IllegalArgumentException("Quarantine duration must be positive: " + quarantineMs);
        }
        this.failedUntil = System.currentTimeMillis() + quarantineMs;
    }

    boolean isAlive() {
        return System.currentTimeMillis() >= failedUntil;
    }
}
