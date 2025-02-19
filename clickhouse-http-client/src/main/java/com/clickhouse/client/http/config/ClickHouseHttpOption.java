package com.clickhouse.client.http.config;

import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;

import java.io.Serializable;
import java.net.UnknownHostException;

/**
 * Http client options.
 */
@Deprecated
public enum ClickHouseHttpOption implements ClickHouseOption {
    /**
     * HTTP connection provider.
     */
    CONNECTION_PROVIDER("http_connection_provider", HttpConnectionProvider.APACHE_HTTP_CLIENT,
            "APACHE HTTP CLIENT connection provider. HTTP_CLIENT is only supported in JDK 11 or above."),
    /**
     * Custom HTTP headers.
     */
    CUSTOM_HEADERS("custom_http_headers", "", "Custom HTTP headers."),
    /**
     * @deprecated use {@link com.clickhouse.client.config.ClickHouseClientOption#CUSTOM_SETTINGS}
     */
    CUSTOM_PARAMS("custom_http_params", "", "Custom HTTP query parameters."),
    /**
     * Default server response.
     */
    DEFAULT_RESPONSE("http_server_default_response", "Ok.\n",
            "Default server response, which is used for validating connection."),
    /**
     * Whether to enable keep-alive or not.
     */
    KEEP_ALIVE("http_keep_alive", true, "Whether to use keep-alive or not"),
    /**
     * Max open connections apply with Apache HttpClient only.
     */
    MAX_OPEN_CONNECTIONS("max_open_connections", 10, "Max open connections apply with Apache HttpClient only."),
    /**
     * Whether to receive information about the progress of a query in response
     * headers.
     */
    RECEIVE_QUERY_PROGRESS("receive_query_progress", true,
            "Whether to receive information about the progress of a query in response headers."),
    /**
     * Indicates whether http client would send its identification through Referer header to server.
     * Valid values:
     *      1. empty string - nothing is sent
     *      2. IP_ADDRESS - client's IP address is used
     *      3. HOST_NAME - host name is used
     */
    SEND_HTTP_CLIENT_ID("send_http_client_id", "", "Indicates whether http client would send its identification through Referer header to server. " +
            "Valid values: empty string - nothing is sent. IP_ADDRESS - client's IP address is used. HOST_NAME - host name is used."),

    // SEND_PROGRESS("send_progress_in_http_headers", false,
    // "Enables or disables X-ClickHouse-Progress HTTP response headers in
    // clickhouse-server responses."),
    // SEND_PROGRESS_INTERVAL("http_headers_progress_interval_ms", 3000, ""),
     WAIT_END_OF_QUERY("wait_end_of_query", false, ""),

    /**
     * Whether to remember last set role and send them in every next requests as query parameters.
     * Only one role can be set at a time.
     */
    REMEMBER_LAST_SET_ROLES("remember_last_set_roles", false,
            "Whether to remember last set role and send them in every next requests as query parameters."),

    /**
     * The time in milliseconds after which the connection is validated after inactivity.
     * Default value is 5000 ms. If set to negative value, the connection is never validated.
     * It is used only for Apache Http Client connection provider.
     */
    AHC_VALIDATE_AFTER_INACTIVITY("ahc_validate_after_inactivity", 5000L,
            "The time in milliseconds after which the connection is validated after inactivity."),

    /**
     * Whether to retry on failure with Apache HTTP Client. Failure includes some 'critical' IO exceptions:
     * <ul>
     *     <li>{@code org.apache.hc.core5.http.ConnectionClosedException}</li>
     *     <li>{@code org.apache.hc.core5.http.NoHttpResponseException}</li>
     * </ul>
     *
     * And next status codes:
     * <ul>
     *     <li>{@code 503 Service Unavailable}</li>
     * </ul>
     */
    AHC_RETRY_ON_FAILURE("ahc_retry_on_failure", false, "Whether to retry on failure with AsyncHttpClient."),

    /**
     * Configuration for Apache HTTP Client connection pool. It defines how to reuse connections.
     * If {@code "FIFO"} is set, the connections are reused in the order they were created.
     * If {@code "LIFO"} is set, the connections are reused as soon they are available.
     * Default value is {@code "LIFO"}.
     */
    CONNECTION_REUSE_STRATEGY("connection_reuse_strategy", "LIFO",
            "Connection reuse strategy for AsyncHttpClient. Valid values: LIFO, FIFO"),

    /**
     * Configures client with preferred connection keep alive timeout if keep alive is enabled.
     * Usually servers tells a client how long it can keep a connection alive. This option can be used
     * when connection should be ended earlier. If value less or equal to 0, the server's timeout is used.
     * Default value is -1.
     * Time unit is milliseconds.
     *
     * Supported only for Apache Http Client connection provider currently.
     */
    KEEP_ALIVE_TIMEOUT("alive_timeout", -1L,
            "Default keep-alive timeout in milliseconds."),

    /**
     * Whether to use HTTP basic authentication. Default value is true.
     * Password that contain UTF8 characters may not be passed through http headers and BASIC authentication
     * is the only option here.
     */
    USE_BASIC_AUTHENTICATION("http_use_basic_auth", true, "Whether to use basic authentication.");

    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;
    private final String description;
    private final boolean sensitive;

    <T extends Serializable> ClickHouseHttpOption(String key, T defaultValue, String description) {
        this(key, defaultValue, description, false);
    }

    <T extends Serializable> ClickHouseHttpOption(String key, T defaultValue, String description, boolean sensitive) {
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
    public Class<? extends Serializable> getValueType() {
        return clazz;
    }

    @Override
    public boolean isSensitive() {
        return sensitive;
    }
}
