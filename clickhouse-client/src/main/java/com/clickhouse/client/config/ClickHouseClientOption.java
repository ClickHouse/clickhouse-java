package com.clickhouse.client.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseFormat;

/**
 * Generic client options.
 */
public enum ClickHouseClientOption implements ClickHouseConfigOption {
    /**
     * Whether the client should run in async mode(e.g.
     * {@link com.clickhouse.client.ClickHouseClient#execute(com.clickhouse.client.ClickHouseRequest)}
     * in a separate thread).
     */
    ASYNC("async", true, "Whether the client should run in async mode."),
    /**
     * Client name.
     */
    CLIENT_NAME("client_name", "ClickHouse Java Client",
            "Client name, which is either 'client_name' or 'http_user_agent' shows up in system.query_log table."),
    /**
     * Case-insensitive transport level compression algorithm. See
     * {@link com.clickhouse.client.ClickHouseCompression} for all possible options.
     */
    COMPRESSION("compression", "LZ4",
            "Transport level compression algorithm used when exchanging data between server and client."),
    /**
     * Connection timeout in milliseconds.
     */
    CONNECTION_TIMEOUT("connect_timeout", 10 * 1000,
            "Connection timeout in milliseconds. It's also used for waiting a connection being closed."),
    /**
     * Default database.
     */
    DATABASE("database", "", "Default database."),
    /**
     * Default format.
     */
    FORMAT("format", ClickHouseFormat.TabSeparated, "Default format."),
    /**
     * Maximum buffer size in byte used for streaming.
     */
    MAX_BUFFER_SIZE("max_buffer_size", 8 * 1024, "Maximum buffer size in byte used for streaming."),
    /**
     * Maximum query execution time in seconds.
     */
    MAX_EXECUTION_TIME("max_execution_time", 0, "Maximum query execution time in seconds."),
    /**
     * Maximum queued in-memory buffers.
     */
    MAX_QUEUED_BUFFERS("max_queued_buffers", 0,
            "Maximum queued in-memory buffers, 0 or negative number means no limit."),
    /**
     * Maxium queued requests. When {@link #MAX_THREADS_PER_CLIENT} is greater than
     * zero, this will also be applied to client's thread pool as well.
     */
    MAX_QUEUED_REQUESTS("max_queued_requests", 0, "Maximum queued requests, 0 or negative number means no limit."),
    /**
     * Maximum rows allowed in the result.
     */
    MAX_RESULT_ROWS("max_result_rows", 0,
            "Limit on the number of rows in the result."
                    + "Also checked for subqueries, and on remote servers when running parts of a distributed query."),
    /**
     * Maximum size of thread pool for each client.
     */
    MAX_THREADS_PER_CLIENT("max_threads_per_client", 0,
            "Size of thread pool for each client instance, 0 or negative number means the client will use shared thread pool."),
    /**
     * Whether to enable retry.
     */
    RETRY("retry", true, "Whether to retry when there's connection issue."),
    /**
     * Whether to reuse wrapper of value.
     */
    REUSE_VALUE_WRAPPER("reuse_value_wrapper", true, "Whether to reuse value-wrapper for memory efficiency."),
    /**
     * Socket timeout in milliseconds.
     */
    SOCKET_TIMEOUT("socket_timeout", 30 * 1000, "Socket timeout in milliseconds."),
    /**
     * Whether to check if session id is validate.
     */
    SESSION_CHECK("session_check", false, "Whether to check if session id is validate."),
    /**
     * Session timeout in milliseconds.
     */
    SESSION_TIMEOUT("session_timeout", 0,
            "Session timeout in milliseconds. 0 or negative number means same as server default."),
    /**
     * Whether to enable SSL for the connection.
     */
    SSL("ssl", false, "enable SSL/TLS for the connection"),
    /**
     * SSL mode.
     */
    SSL_MODE("sslmode", ClickHouseSslMode.STRICT, "verify or not certificate: none (don't verify), strict (verify)"),
    /**
     * SSL root certificiate.
     */
    SSL_ROOT_CERTIFICATE("sslrootcert", "", "SSL/TLS root certificate"),
    /**
     * SSL certificiate.
     */
    SSL_CERTIFICATE("sslcert", "", "SSL/TLS certificate"),
    /**
     * SSL key.
     */
    SSL_KEY("sslkey", "", "SSL/TLS key"),
    /**
     * Whether to use objects in array or not.
     */
    USE_OBJECTS_IN_ARRAYS("use_objects_in_arrays", false, "Whether Object[] should be used instead primitive arrays."),
    /**
     * Whether to use server time zone.
     */
    USE_SERVER_TIME_ZONE("use_server_time_zone", true,
            "Whether to use time zone from server. On connection init select timezone() will be executed"),
    /**
     * Whether to use time zone from server for Date.
     */
    USE_SERVER_TIME_ZONE_FOR_DATES("use_server_time_zone_for_dates", false,
            "Whether to use time zone from server on Date parsing in getDate(). "
                    + "If false, Date returned is a wrapper of a timestamp at start of the day in client time zone. "
                    + "If true - at start of the day in server or use_timezone time zone."),
    /**
     * Custom time zone. Only works when {@code use_server_time_zone} is set to
     * false.
     */
    USE_TIME_ZONE("use_time_zone", "", "Which time zone to use");

    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;
    private final String description;

    private static final Map<String, ClickHouseClientOption> options;

    static {
        Map<String, ClickHouseClientOption> map = new HashMap<>();

        for (ClickHouseClientOption o : values()) {
            if (map.put(o.getKey(), o) != null) {
                throw new IllegalStateException("Duplicated key found: " + o.getKey());
            }
        }

        options = Collections.unmodifiableMap(map);
    }

    /**
     * Gets client option by key.
     *
     * @param key key of the option
     * @return client option object, or null if not found
     */
    public static ClickHouseClientOption fromKey(String key) {
        return options.get(key);
    }

    /**
     * Constructor of an option for client.
     *
     * @param <T>          type of the value, usually either String or Number(e.g.
     *                     int, long etc.)
     * @param key          non-null key, better use snake_case instead of camelCase
     *                     for consistency
     * @param defaultValue non-null default value
     * @param description  non-null description of this option
     */
    <T extends Serializable> ClickHouseClientOption(String key, T defaultValue, String description) {
        this.key = ClickHouseChecker.nonNull(key, "key");
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
        this.description = ClickHouseChecker.nonNull(description, "description");
    }

    @Override
    public Serializable getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Class<? extends Serializable> getValueType() {
        return clazz;
    }
}
