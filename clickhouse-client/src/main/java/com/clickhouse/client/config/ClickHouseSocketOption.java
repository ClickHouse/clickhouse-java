package com.clickhouse.client.config;

import com.clickhouse.client.ClickHouseChecker;

import java.io.Serializable;
import java.net.SocketOption;

/**
 * Socket related options.
 */
public enum ClickHouseSocketOption implements ClickHouseOption {

    /**
     * Socket IP_TOS option which indicates IP package priority.
     */
    IP_TOS("socket_op_ip_tos", 0, "Socket IP_TOS option which indicates IP package priority.");

    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;
    private final String description;

    <T extends Serializable> ClickHouseSocketOption(String key, T defaultValue, String description) {
        this.key = ClickHouseChecker.nonNull(key, "key");
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
        this.description = ClickHouseChecker.nonNull(description, "description");
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
}
