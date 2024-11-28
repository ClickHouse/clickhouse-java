package com.clickhouse.jdbc.internal;

public enum ClientInfoProperties {

    APPLICATION_NAME("ApplicationName", 255, "", "Client application name."),
    ;

    private String key;
    private int maxValue;

    private String defaultValue;

    private String description;

    ClientInfoProperties(String key, int maxValue, String defaultValue, String description) {
        this.key = key;
        this.maxValue = maxValue;
        this.defaultValue = defaultValue;
        this.description = description;
    }

    public String getKey() {
        return key;
    }

    public int getMaxValue() {
        return maxValue;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }
}
