package com.clickhouse.jdbc.internal;

import java.util.Collections;
import java.util.List;

/**
 * JDBC driver specific properties. Should not include any of ClientConfigProperties.
 * All driver specific properties must have {@code "driver."} prefix to isolate them from everything else
 */
public enum DriverProperties {

    /**
     * query settings to be passed along with query operation.
     * {@see com.clickhouse.client.api.query.QuerySettings}
     */
    DEFAULT_QUERY_SETTINGS("driver.query_settings", null);

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
