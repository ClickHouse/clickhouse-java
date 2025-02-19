package com.clickhouse.config;

import java.io.Serializable;

/**
 * Configuration change listener.
 */
@Deprecated
public interface ClickHouseConfigChangeListener<T> {
    /**
     * Triggered when {@link ClickHouseOption} was changed. Removing an option is
     * same as reseting its value to null.
     *
     * @param source   source of the event
     * @param option   the changed option, which should never be null
     * @param oldValue old option value, which could be null
     * @param newValue new option value, which could be null
     */
    default void optionChanged(T source, ClickHouseOption option, Serializable oldValue, Serializable newValue) {
    }

    /**
     * Triggered when property of {@code source} was changed.
     *
     * @param source   source of the event
     * @param property name of the changed property, which should never be null
     * @param oldValue old option value, which could be null
     * @param newValue new option value, which could be null
     */
    default void propertyChanged(T source, String property, Object oldValue, Object newValue) {
    }

    /**
     * Triggered when ClickHouse setting(declared on client-side) was changed.
     * Removing a setting is same as reseting its value to {@code null}.
     *
     * @param source   source of the event
     * @param setting  the changed setting, which should never be null
     * @param oldValue old option value, which could be null
     * @param newValue new option value, which could be null
     */
    default void settingChanged(T source, String setting, Serializable oldValue, Serializable newValue) {
    }
}
