package com.clickhouse.examples.model;

import java.time.Instant;
import java.util.UUID;

/**
 * One-second view of mean signal values reported by devices in a location.
 */
public record SignalSlice(
        UUID locationId,
        Instant timestamp,
        Double temperature,
        Double humidity,
        Double pressure,
        Double motion,
        Double gas,
        Double battery,
        Double light
) {
}
