package com.clickhouse.client.config;

/**
 * Defines supported SSL mode.
 */
@Deprecated
public enum ClickHouseProxyType {
    IGNORE, DIRECT, HTTP, SOCKS;
}
