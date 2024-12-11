package com.clickhouse.jdbc.internal;

/**
 * JDBC driver specific properties. Do not include any ClickHouse client properties here.
 */
public enum DriverProperties {

    IGNORE_UNSUPPORTED_VALUES("jdbc_ignore_unsupported_values", ""),
    SCHEMA_TERM("jdbc_schema_term", ""),
    PLACEHOLDER("placeholder", "Placeholder for unknown properties");

    private final String key;

    private final String description;

    DriverProperties(String key, String description) {
        this.key = key;
        this.description = description;
    }

    public String getKey() {
        return key;
    }
}
