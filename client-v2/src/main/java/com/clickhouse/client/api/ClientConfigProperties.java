package com.clickhouse.client.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Enumerates all client properties that are known at release.
 */
public enum ClientConfigProperties {

    SESSION_DB_ROLES("session_db_roles"),

    SETTING_LOG_COMMENT(serverSetting("log_comment")),

    HTTP_USE_BASIC_AUTH("http_use_basic_auth"),

    USER("user", "default"),

    PASSWORD("password", ""),

    /**
     * Maximum number of active connection in internal connection pool.
     */
    HTTP_MAX_OPEN_CONNECTIONS("max_open_connections", "10"),

    /**
     * HTTP keep-alive timeout override.
     */
    HTTP_KEEP_ALIVE_TIMEOUT("http_keep_alive_timeout"),

    USE_SERVER_TIMEZONE("use_server_time_zone"),

    USE_TIMEZONE("use_time_zone"),

    SERVER_TIMEZONE("server_time_zone"),

    ASYNC_OPERATIONS("async"),

    CONNECTION_TTL("connection_ttl"),

    CONNECTION_TIMEOUT("connection_timeout"),

    CONNECTION_REUSE_STRATEGY("connection_reuse_strategy"),

    SOCKET_OPERATION_TIMEOUT("socket_timeout"),

    SOCKET_RCVBUF_OPT("socket_rcvbuf"),

    SOCKET_SNDBUF_OPT("socket_sndbuf"),

    SOCKET_REUSEADDR_OPT("socket_reuseaddr"),

    SOCKET_KEEPALIVE_OPT("socket_keepalive"),

    SOCKET_TCP_NO_DELAY_OPT("socket_tcp_nodelay"),

    SOCKET_LINGER_OPT("socket_linger"),

    DATABASE("database", "default"),

    COMPRESS_SERVER_RESPONSE("compress"), // actually a server setting, but has client effect too

    COMPRESS_CLIENT_REQUEST("decompress"), // actually a server setting, but has client effect too

    USE_HTTP_COMPRESSION("client.use_http_compression"),

    COMPRESSION_LZ4_UNCOMPRESSED_BUF_SIZE("compression.lz4.uncompressed_buffer_size"),

    DISABLE_NATIVE_COMPRESSION("disable_native_compression", "false"),

    PROXY_TYPE("proxy_type"), // "http"

    PROXY_HOST("proxy_host"),

    PROXY_PORT("proxy_port"),

    PROXY_USER("proxy_user"),

    PROXY_PASSWORD("proxy_password"),

    MAX_EXECUTION_TIME("max_execution_time"),

    SSL_TRUST_STORE("trust_store"),

    SSL_KEYSTORE_TYPE("key_store_type"),

    SSL_KEY_STORE("ssl_key_store"),

    SSL_KEY_STORE_PASSWORD("key_store_password"),

    SSL_KEY("ssl_key"),

    CA_CERTIFICATE("sslrootcert"),

    SSL_CERTIFICATE("sslcert"),

    RETRY_ON_FAILURE("retry"),

    INPUT_OUTPUT_FORMAT("format"),

    MAX_THREADS_PER_CLIENT("max_threads_per_client"),

    QUERY_ID("query_id"), // actually a server setting, but has client effect too

    CLIENT_NETWORK_BUFFER_SIZE("client_network_buffer_size", String.valueOf(Client.Builder.DEFAULT_BUFFER_SIZE)),

    ACCESS_TOKEN("access_token"), SSL_AUTH("ssl_authentication"),

    CONNECTION_POOL_ENABLED("connection_pool_enabled"),

    CONNECTION_REQUEST_TIMEOUT("connection_request_timeout"),

    CLIENT_RETRY_ON_FAILURE("client_retry_on_failures"),

    CLIENT_NAME("client_name"),

    /**
     * An old alias to {@link ClientConfigProperties#CLIENT_NAME}. Using the last one is preferred.
     */
    @Deprecated
    PRODUCT_NAME("product_name"),

    BEARERTOKEN_AUTH ("bearer_token"),
    /**
     * Indicates that data provided for write operation is compressed by application.
     */
    APP_COMPRESSED_DATA("app_compressed_data"),
    /**
     *
     */
    METRICS_GROUP_NAME("metrics_name"),
    ;

    private String key;

    private String defaultValue;

    private List<String> choices;


    ClientConfigProperties(String key) {
        this(key, null, Collections.emptyList());
    }

    ClientConfigProperties(String key, String defaultValue) {
        this(key, defaultValue, Collections.emptyList());
    }

    ClientConfigProperties(String key, String defaultValue, List<String> choices) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.choices = Collections.unmodifiableList(choices);
    }

    public String getKey() {
        return key;
    }

    public List<String> getChoices() {
        return choices;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public static final String HTTP_HEADER_PREFIX = "http_header_";

    public static final String SERVER_SETTING_PREFIX = "clickhouse_setting_";

    public static String serverSetting(String key) {
        return SERVER_SETTING_PREFIX + key;
    }

    public static String httpHeader(String key) {
        return HTTP_HEADER_PREFIX + key.toUpperCase(Locale.US);
    }

    public static String commaSeparated(Collection<?> values) {
        StringBuilder sb = new StringBuilder();
        for (Object value : values) {
            sb.append(value.toString().replaceAll(",", "\\\\,")).append(",");
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public static List<String> valuesFromCommaSeparated(String value) {
        if (value == null || value.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(value.split("(?<!\\\\),")).map(s -> s.replaceAll("\\\\,", ","))
                .collect(Collectors.toList());
    }
}
