package com.clickhouse.client.config;

import java.io.Serializable;

import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;

/**
 * System-wide default options. System properties and environment variables can
 * be set to change default value.
 * 
 * <p>
 * For example, by default {@link #ASYNC} is set to {@code true}. However, you
 * can change it to {@code false} by either specifying
 * {@code -Ddefault_async=false} on the Java command line, or setting
 * environment variable {@code DEFAULT_ASYNC=false}.
 */
@Deprecated
public enum ClickHouseDefaults implements ClickHouseOption {
    /**
     * Default execution mode.
     */
    ASYNC("async", ClickHouseDataConfig.DEFAULT_ASYNC, "Whether the client should run in async mode."),
    /**
     * Whether to create session automatically when there are multiple queries.
     */
    AUTO_SESSION("auto_session", true, "Whether to create session automatically when there are multiple queries."),
    /**
     * Default buffering mode.
     */
    BUFFERING("buffering", ClickHouseDataConfig.DEFAULT_BUFFERING_MODE, "Buffering mode."),
    /**
     * Default server host.
     */
    HOST("host", "localhost", "Host to connect to."),
    /**
     * Default protocol.
     */
    PROTOCOL("protocol", ClickHouseProtocol.ANY, "Protocol to use."),
    /**
     * Default database.
     */
    DATABASE("database", "default", "Database to connect to."),
    /**
     * Default user.
     */
    USER("user", "default", "User name for authentication."),
    /**
     * Default password.
     */
    PASSWORD("password", "", "Password for authentication.", true),
    /**
     * Default format.
     */
    FORMAT("format", ClickHouseDataConfig.DEFAULT_FORMAT,
            "Preferred data format for serialization and deserialization."),
    /**
     * Maximum number of threads that the scheduler(shared by all client instances)
     * can use to run the adhoc/scheduled tasks like discovery and health check.
     */
    MAX_SCHEDULER_THREADS("max_scheduler_threads", 1,
            "Maximum number of threads that the scheduler(shared by all client instances) can use to run the adhoc/scheduled tasks like discovery and health check.."),
    /**
     * Max threads.
     */
    MAX_THREADS("max_threads", 0, "Maximum size of shared thread pool, 0 or negative number means same as CPU cores."),
    /**
     * Max requests.
     */
    MAX_REQUESTS("max_requests", 0, "Maximum size of shared thread pool, 0 means no limit."),
    /**
     * Rounding mode for type conversion.
     */
    ROUNDING_MODE("rounding_mode", ClickHouseDataConfig.DEFAULT_ROUNDING_MODE, "Default rounding mode for BigDecimal."),
    /**
     * Thread keep alive timeout in milliseconds.
     */
    THREAD_KEEPALIVE_TIMEOUT("thread_keepalive_timeout", 0L,
            "Thread keep alive timeout in milliseconds. 0 or negative number means additional thread will be closed immediately after execution completed."),
    /**
     * Server time zone, defaults to {@code UTC}.
     */
    SERVER_TIME_ZONE("time_zone", "UTC", "Server time zone."),
    /**
     * Server version, defaults to {@code latest}.
     */
    SERVER_VERSION("version", "latest", "Server version"),
    /**
     * SSL certificiate type.
     */
    SSL_CERTIFICATE_TYPE("sslcerttype", "X.509", "SSL/TLS certificate type."),
    /**
     * SSL key algorithm.
     */
    SSL_KEY_ALGORITHM("sslkeyalg", "RSA", "Key algorithm."),
    /**
     * SSL key.
     */
    SSL_PROTOCOL("sslprotocol", "TLS", "SSL protocol."),
    /**
     * Whether to resolve DNS SRV name using
     * {@link com.clickhouse.client.naming.SrvResolver}(e.g. resolve SRV record to
     * extract both host and port from a given name).
     */
    SRV_RESOLVE("srv_resolve", false, "Whether to resolve DNS SRV name.");

    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;
    private final String description;
    private final boolean sensitive;

    <T extends Serializable> ClickHouseDefaults(String key, T defaultValue, String description) {
        this(key, defaultValue, description, false);
    }

    <T extends Serializable> ClickHouseDefaults(String key, T defaultValue, String description, boolean sensitive) {
        this.key = ClickHouseChecker.nonNull(key, "key");
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
        this.description = ClickHouseChecker.nonNull(description, "description");
        this.sensitive = sensitive;
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
    public String getPrefix() {
        return "DEFAULT";
    }

    @Override
    public Class<? extends Serializable> getValueType() {
        return clazz;
    }

    @Override
    public boolean isSensitive() {
        return sensitive;
    }
}
