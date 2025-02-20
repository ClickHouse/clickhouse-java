package com.clickhouse.client.config;

@Deprecated
public enum ClickHouseHealthCheckMethod {
    /**
     * Ping is the protocol-specific approach for health check, which in general is
     * faster.
     */
    PING,
    /**
     * Issue query "select 1" for health check, slightly slower compare to ping but
     * works the best with 3party tools.
     */
    SELECT_ONE;
}
