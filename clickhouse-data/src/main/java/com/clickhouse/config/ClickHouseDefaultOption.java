package com.clickhouse.config;

import java.io.Serializable;
import java.util.Locale;

import com.clickhouse.data.ClickHouseChecker;

@Deprecated
public final class ClickHouseDefaultOption implements ClickHouseOption {
    private final String name;
    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;
    private final boolean sensitive;

    public <T extends Serializable> ClickHouseDefaultOption(String name, T defaultValue) {
        this(name, defaultValue, false);
    }

    public <T extends Serializable> ClickHouseDefaultOption(String name, T defaultValue, boolean sensitive) {
        this.name = ClickHouseChecker.nonNull(name, "name").toUpperCase(Locale.ROOT);
        this.key = name.toLowerCase(Locale.ROOT);
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
        this.sensitive = sensitive;
    }

    @Override
    public Serializable getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getDescription() {
        return "";
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

    @Override
    public String name() {
        return name;
    }
}
