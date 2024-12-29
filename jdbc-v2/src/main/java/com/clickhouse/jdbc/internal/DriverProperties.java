package com.clickhouse.jdbc.internal;

import java.util.Collections;
import java.util.List;

/**
 * JDBC driver specific properties. Should not include any of ClientConfigProperties.
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
     * query settings to be passed along with query operation.
     * {@see com.clickhouse.client.api.query.QuerySettings}
     */
    DEFAULT_QUERY_SETTINGS("default_query_settings", null);
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
