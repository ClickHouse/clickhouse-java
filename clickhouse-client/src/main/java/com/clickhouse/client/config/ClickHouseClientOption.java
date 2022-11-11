package com.clickhouse.client.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseFormat;

/**
 * Generic client options.
 */
public enum ClickHouseClientOption implements ClickHouseOption {
    /**
     * Whether the client should run in async mode(e.g.
     * {@link com.clickhouse.client.ClickHouseClient#execute(com.clickhouse.client.ClickHouseRequest)}
     * in a separate thread).
     */
    ASYNC("async", true, "Whether the client should run in async mode."),
    /**
     * Whether the client should discover more nodes from system tables and/or
     * clickhouse-keeper/zookeeper.
     */
    AUTO_DISCOVERY("auto_discovery", false,
            "Whether the client should discover more nodes from system tables and/or clickhouse-keeper/zookeeper."),
    /**
     * Custom server settings for all queries.
     */
    CUSTOM_SETTINGS("custom_settings", "", "Custom server settings for all queries."),
    /**
     * Load balancing policy.
     */
    LOAD_BALANCING_POLICY("load_balancing_policy", "",
            "Load balancing policy, can be one of '', 'firstAlive', 'random', 'roundRobin', or full qualified class name implementing ClickHouseLoadBalancingPolicy."),
    /**
     * Load balancing tags for filtering out nodes.
     */
    LOAD_BALANCING_TAGS("load_balancing_tags", "", "Load balancing tags for filtering out nodes."),
    /**
     * Health check interval in milliseconds.
     */
    HEALTH_CHECK_INTERVAL("health_check_interval", 0,
            "Health check interval in milliseconds, zero or negative value means one-time."),
    /**
     * Health check method.
     */
    HEALTH_CHECK_METHOD("health_check_method", ClickHouseHealthCheckMethod.SELECT_ONE, "Health check method."),
    /**
     * Node discovery interval in milliseconds.
     */
    NODE_DISCOVERY_INTERVAL("node_discovery_interval", 0,
            "Node discovery interval in milliseconds, zero or negative value means one-time discovery."),
    /**
     * Maximum number of nodes can be discovered at a time.
     */
    NODE_DISCOVERY_LIMIT("node_discovery_limit", 100,
            "Maximum number of nodes can be discovered at a time, zero or negative value means no limit."),
    /**
     * Node check interval in milliseconds.
     */
    NODE_CHECK_INTERVAL("node_check_interval", 0,
            "Node check interval in milliseconds, negative number is treated as zero."),
    /**
     * Maximum number of nodes can be used for operation at a time.
     */
    NODE_GROUP_SIZE("node_group_size", 50,
            "Maximum number of nodes can be used for operation at a time, zero or negative value means all."),
    /**
     * Whether to perform health check against all nodes or just faulty ones.
     */
    CHECK_ALL_NODES("check_all_nodes", false,
            "Whether to perform health check against all nodes or just faulty ones."),
    /**
     * Default buffer size in byte for both request and response. It will be reset
     * to {@link #MAX_BUFFER_SIZE} if it's too large.
     */
    BUFFER_SIZE("buffer_size", 8192, "Default buffer size in byte for both request and response."),
    /**
     * Number of times the buffer queue is filled up before increasing capacity of
     * buffer queue. Zero or negative value means the queue length is fixed.
     */
    BUFFER_QUEUE_VARIATION("buffer_queue_variation", 100,
            "Number of times the buffer queue is filled up before increasing capacity of buffer queue. Zero or negative value means the queue length is fixed."),
    /**
     * Read buffer size in byte. It's mainly for input stream(e.g. reading data from
     * server response). Its value defaults to {@link #BUFFER_SIZE}, and it will be
     * reset to {@link #MAX_BUFFER_SIZE} when it's too large.
     */
    READ_BUFFER_SIZE("read_buffer_size", 0,
            "Read buffer size in byte, zero or negative value means same as buffer_size"),
    /**
     * Write buffer size in byte. It's mainly for output stream(e.g. writing data
     * into request). Its value defaults to {@link #BUFFER_SIZE}, and it will
     * be reset to {@link #MAX_BUFFER_SIZE} when it's too large.
     */
    WRITE_BUFFER_SIZE("write_buffer_size", 0,
            "Write buffer size in byte, zero or negative value means same as buffer_size"),
    /**
     * Maximum request chunk size in byte.
     */
    REQUEST_CHUNK_SIZE("request_chunk_size", 0,
            "Maximum request chunk size in byte, zero or negative value means same as write_buffer_size"),
    /**
     * Request buffering mode.
     */
    REQUEST_BUFFERING("request_buffering", ClickHouseDefaults.BUFFERING.getDefaultValue(),
            "Request buffering mode"),
    /**
     * Response buffering mode.
     */
    RESPONSE_BUFFERING("response_buffering", ClickHouseDefaults.BUFFERING.getDefaultValue(),
            "Response buffering mode."),
    /**
     * Client name.
     */
    CLIENT_NAME("client_name", "ClickHouse Java Client",
            "Client name, which is either 'client_name' or 'http_user_agent' shows up in system.query_log table."),
    /**
     * Whether server will compress response to client or not.
     */
    COMPRESS("compress", true, "Whether the server will compress response it sends to client."),
    /**
     * Whether server will decompress request from client or not.
     */
    DECOMPRESS("decompress", false, "Whether the server will decompress request from client."),
    /**
     * Compression algorithm server will use to compress response, when
     * {@link #COMPRESS} is {@code true}.
     */
    COMPRESS_ALGORITHM("compress_alogrithm", ClickHouseCompression.LZ4,
            "Algorithm used for server to compress response."),
    /**
     * Compression algorithm server will use to decompress request, when
     * {@link #DECOMPRESS} is {@code true}.
     */
    DECOMPRESS_ALGORITHM("decompress_alogrithm", ClickHouseCompression.LZ4,
            "Algorithm for server to decompress request."),
    /**
     * Compression level for compressing server response.
     */
    COMPRESS_LEVEL("compress_level", 3, "Compression level for response, from 0 to 9(low to high)"),
    /**
     * Compression level for decompress client request.
     */
    DECOMPRESS_LEVEL("decompress_level", 3, "Compression level for request, from 0 to 9(low to high)"),

    /**
     * Connection timeout in milliseconds.
     */
    CONNECTION_TIMEOUT("connect_timeout", 5000,
            "Connection timeout in milliseconds. It's also used for waiting a connection being closed."),
    /**
     * Default database.
     */
    DATABASE("database", "", "Default database."),
    /**
     * Maximum number of times failover can happen for a request.
     */
    FAILOVER("failover", 0,
            "Maximum number of times failover can happen for a request, zero or negative value means no failover."),
    /**
     * Default format.
     */
    FORMAT("format", ClickHouseFormat.TabSeparated, "Default format."),
    /**
     * Whether to log leading comment(as log_comment in system.query_log) of the
     * query.
     */
    LOG_LEADING_COMMENT("log_leading_comment", false,
            "Whether to log leading comment(as log_comment in system.query_log) of the query."),
    /**
     * Maximum buffer size in byte can be used for streaming. It's not supposed to
     * be larger than {@code Integer.MAX_VALUE - 8}.
     */
    MAX_BUFFER_SIZE("max_buffer_size", 1024 * 1024 * 1024,
            "Maximum buffer size in byte can be used for streaming."),
    /**
     * Maximum query execution time in seconds.
     */
    MAX_EXECUTION_TIME("max_execution_time", 0, "Maximum query execution time in seconds, 0 means no limit."),
    /**
     * Maximum queued in-memory buffers.
     */
    MAX_QUEUED_BUFFERS("max_queued_buffers", 512,
            "Maximum queued in-memory buffers, 0 or negative number means no limit."),
    /**
     * Maxium queued requests. When {@link #MAX_THREADS_PER_CLIENT} is greater than
     * zero, this will also be applied to client's thread pool as well.
     */
    MAX_QUEUED_REQUESTS("max_queued_requests", 0, "Maximum queued requests, 0 or negative number means no limit."),
    /**
     * Maximum rows allowed in the result.
     */
    MAX_RESULT_ROWS("max_result_rows", 0L,
            "Limit on the number of rows in the result. "
                    + "Also checked for subqueries, and on remote servers when running parts of a distributed query."),
    /**
     * Maximum size of thread pool for each client.
     */
    MAX_THREADS_PER_CLIENT("max_threads_per_client", 0,
            "Size of thread pool for each client instance, 0 or negative number means the client will use shared thread pool."),
    /**
     * Method to rename response columns.
     */
    RENAME_RESPONSE_COLUMN("rename_response_column", ClickHouseRenameMethod.NONE,
            "Method to rename response columns."),
    /**
     * Maximum number of times retry can happen for a request.
     */
    RETRY("retry", 0,
            "Maximum number of times retry can happen for a request, zero or negative value means no retry."),
    /**
     * Whether to repeat execution when session is locked, until timed out(according
     * to {@link #SESSION_TIMEOUT} or {@link #CONNECTION_TIMEOUT}).
     */
    REPEAT_ON_SESSION_LOCK("repeat_on_session_lock", true,
            "Whether to repeat execution when session is locked, until timed out(according to 'session_timeout' or 'connect_timeout')."),
    /**
     * Whether to reuse wrapper of value(e.g. ClickHouseValue or
     * ClickHouseRecord) for memory efficiency.
     */
    REUSE_VALUE_WRAPPER("reuse_value_wrapper", true,
            "Whether to reuse wrapper of value(e.g. ClickHouseValue or ClickHouseRecord) for memory efficiency."),
    /**
     * Server revision.
     */
    SERVER_REVISION("server_revision", 54442, "Server revision."),
    /**
     * Server timezone.
     */
    SERVER_TIME_ZONE("server_time_zone", "", "Server timezone."),
    /**
     * Server version.
     */
    SERVER_VERSION("server_version", "", "Server version."),
    /**
     * Session id.
     */
    SESSION_ID("session_id", "", "Session id"),
    /**
     * Whether to check if session id is validate.
     */
    SESSION_CHECK("session_check", false, "Whether to check if existence of session id."),
    /**
     * Session timeout in seconds.
     */
    SESSION_TIMEOUT("session_timeout", 0,
            "Session timeout in seconds. 0 or negative number means same as server default."),
    /**
     * Socket timeout in milliseconds.
     */
    SOCKET_TIMEOUT("socket_timeout", 30 * 1000, "Socket timeout in milliseconds."),
    /**
     * Whether to enable SSL for the connection.
     */
    SSL("ssl", false, "Whether to enable SSL/TLS for the connection."),
    /**
     * SSL mode.
     */
    SSL_MODE("sslmode", ClickHouseSslMode.STRICT,
            "verify or not certificate: none (don't verify), strict (verify)"),
    /**
     * SSL root certificiate.
     */
    SSL_ROOT_CERTIFICATE("sslrootcert", "", "SSL/TLS root certificates."),
    /**
     * SSL certificiate.
     */
    SSL_CERTIFICATE("sslcert", "", "SSL/TLS certificate."),
    /**
     * SSL key.
     */
    SSL_KEY("sslkey", "", "RSA key in PKCS#8 format."),
    /**
     * Transaction timeout in seconds.
     */
    TRANSACTION_TIMEOUT("transaction_timeout", 0,
            "Transaction timeout in seconds. 0 or negative number means same as session_timeout."),
    /**
     * Whether to convert unsigned types to the next widest type(e.g. use
     * {@code short} for UInt8 instead of {@code byte}, and {@code UnsignedLong} for
     * UInt64).
     */
    WIDEN_UNSIGNED_TYPES("widen_unsigned_types", false,
            "Whether to convert unsigned types to the next widest type(e.g. use short for UInt8 instead of byte, and UnsignedLong for UInt64)."),
    /**
     * Whether to support binary string. Enable this option to treat
     * {@code FixedString} and {@code String} as byte array.
     */
    USE_BINARY_STRING("use_binary_string", false, "Whether to support binary string. "
            + "Enable this option to treat FixedString and String as byte array."),
    /**
     * Whether to use blocking queue for buffering.
     */
    USE_BLOCKING_QUEUE("use_blocking_queue", true, "Whether to use blocking queue for buffering."),
    /**
     * Whether Object[] should be used instead of primitive arrays.
     */
    USE_OBJECTS_IN_ARRAYS("use_objects_in_arrays", false,
            "Whether Object[] should be used instead of primitive arrays."),
    /**
     * Whether to access ClickHouse server directly without using system wide proxy
     * including the one defined in JVM system properties.
     */
    USE_NO_PROXY("use_no_proxy", false,
            "Whether to access ClickHouse server directly without using system wide proxy including the one defined in JVM system properties."),
    /**
     * Whether to use server time zone.
     */
    USE_SERVER_TIME_ZONE("use_server_time_zone", true,
            "Whether to use server time zone. On connection init select timezone() will be executed"),
    /**
     * Whether to use time zone from server for Date.
     */
    USE_SERVER_TIME_ZONE_FOR_DATES("use_server_time_zone_for_dates", false,
            "Whether to use timezone from server on Date parsing in getDate(). "
                    + "If false, Date returned is a wrapper of a timestamp at start of the day in client timezone. "
                    + "If true - at start of the day in server or use_time_zone timezone."),
    /**
     * Custom time zone. Only works when {@code use_server_time_zone} is set to
     * false.
     */
    USE_TIME_ZONE("use_time_zone", "", "Time zone of all DateTime* values. "
            + "Only used when use_server_time_zone is false. Empty value means client time zone.");

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
