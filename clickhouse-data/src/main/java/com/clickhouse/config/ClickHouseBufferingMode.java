package com.clickhouse.config;

/**
 * Supported buffering mode for dealing with request and response.
 */
@Deprecated
public enum ClickHouseBufferingMode {
    // TODO Adaptive / Dynamic

    /**
     * Resource-efficient mode provides reasonable performance with least CPU and
     * memory usage, which makes it ideal as default mode. Only buffer size is
     * considered in this mode, no queue is used for buffering.
     */
    RESOURCE_EFFICIENT,
    /**
     * Custom mode allows you to customize buffer size and queue to balance resource
     * utilization and desired performance.
     */
    CUSTOM,
    /**
     * Performance mode provides best performance at the cost of more CPU and much
     * much more memory usage - almost everything is loaded into working memory.
     */
    PERFORMANCE
}
