package com.clickhouse.jdbc;

import com.clickhouse.client.api.internal.ServerSettings;

import java.util.Collections;
import java.util.List;

/**
 * JDBC driver specific properties. Does not include anything from ClientConfigProperties.
 * Processing logic should be the follows
 * 1. If property is among DriverProperties then Driver handles it specially and will not pass to a client
 * 2. If property is not among DriverProperties then it is passed to a client
 */
public enum DriverProperties {

    IGNORE_UNSUPPORTED_VALUES("jdbc_ignore_unsupported_values", ""),
    SCHEMA_TERM("jdbc_schema_term", ""),
    /**
     * Indicates if driver should create a secure connection over SSL/TLS
     */
    SECURE_CONNECTION("ssl", "false"),

    /**
     * Query settings to be passed along with query operation.
     * {@see com.clickhouse.client.api.query.QuerySettings}
     */
    DEFAULT_QUERY_SETTINGS("default_query_settings", null),

    /**
     * Enables row binary writer for simple insert statements when
     * PreparedStatement is used. Has limitation and can be used with a simple form of insert like;
     * {@code INSERT INTO t VALUES (?, ?, ?...)}
     */
    BETA_ROW_BINARY_WRITER("beta.row_binary_for_simple_insert", "false"),

    /**
     *  Enables closing result set before
     */
    RESULTSET_AUTO_CLOSE("jdbc_resultset_auto_close", "true"),

    /**
     * Enables using server property `max_result_rows` ({@link ServerSettings#MAX_RESULT_ROWS} to limit number of rows returned by query.
     * Enabling this property will override user set overflow mode. It may cause error if server doesn't allow changing properties.
     * When this property is not enabled then result set will stop reading data once limit is reached. As server may have
     * more in a result set then it will require time to read all data to make HTTP connection usable again. In most cases
     * this is fine. It is recommended to set limit in SQL query.
     *
     */
    USE_MAX_RESULT_ROWS("jdbc_use_max_result_rows", "false"),
    ;


    private final String key;

    private final String defaultValue;

    private final List<String> choices;

    DriverProperties(String key, String defaultValue) {
        this(key, defaultValue, Collections.emptyList());
    }

    DriverProperties(String key, String defaultValue, List<String> choices) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.choices = choices;
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public List<String> getChoices() {
        return choices;
    }
}
