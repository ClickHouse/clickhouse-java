package com.clickhouse.client.http.config;

import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;

import java.io.Serializable;

/**
 * Http client options.
 */
public enum ClickHouseHttpOption implements ClickHouseOption {
    /**
     * HTTP connection provider.
     */
    CONNECTION_PROVIDER("http_connection_provider", HttpConnectionProvider.HTTP_URL_CONNECTION,
            "HTTP connection provider. HTTP_CLIENT is only supported in JDK 11 or above."),
    /**
     * Custom HTTP headers.
     */
    CUSTOM_HEADERS("custom_http_headers", "", "Custom HTTP headers."),
    /**
     * Custom HTTP query parameters. Consider
     * {@link com.clickhouse.client.config.ClickHouseClientOption#CUSTOM_SETTINGS}
     * if you don't want your implementation ties to http protocol.
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
     * Whether to receive information about the progress of a query in response
     * headers.
     */
    RECEIVE_QUERY_PROGRESS("receive_query_progress", true,
            "Whether to receive information about the progress of a query in response headers."),
    // SEND_PROGRESS("send_progress_in_http_headers", false,
    // "Enables or disables X-ClickHouse-Progress HTTP response headers in
    // clickhouse-server responses."),
    // SEND_PROGRESS_INTERVAL("http_headers_progress_interval_ms", 3000, ""),
    // WAIT_END_OF_QUERY("wait_end_of_query", false, ""),
    /**
     * Web context.
     */
    WEB_CONTEXT("web_context", "/", "Web context.");

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
