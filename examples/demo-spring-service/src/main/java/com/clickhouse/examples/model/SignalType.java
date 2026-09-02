package com.clickhouse.examples.model;

/**
 * The kinds of signal an IoT device may emit.
 *
 * <p>Kept as a closed enum so that unknown signal types are rejected at the edge
 * and so that per-type metrics have a bounded, low-cardinality set of values.
 */
public enum SignalType {
    TEMPERATURE,
    HUMIDITY,
    PRESSURE,
    MOTION,
    GAS,
    BATTERY,
    LIGHT
}
