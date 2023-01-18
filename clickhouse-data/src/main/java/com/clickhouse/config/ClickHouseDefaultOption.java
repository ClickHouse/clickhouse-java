package com.clickhouse.config;

import java.io.Serializable;
import java.util.Locale;

import com.clickhouse.data.ClickHouseChecker;

public final class ClickHouseDefaultOption implements ClickHouseOption {
    private final String name;
    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;

    public <T extends Serializable> ClickHouseDefaultOption(String name, T defaultValue) {
        this.name = ClickHouseChecker.nonNull(name, "name").toUpperCase(Locale.ROOT);
        this.key = name.toLowerCase(Locale.ROOT);
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
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
    public String name() {
        return name;
    }
}
