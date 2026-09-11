package com.clickhouse.examples.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * A single reading emitted by an IoT device, as received over HTTP.
 *
 * @param deviceId   identifier of the emitting device (required)
 * @param locationId identifier of the device location (required)
 * @param type       kind of measurement (required, must be a known {@link SignalType})
 * @param value      numeric reading
 * @param unit       unit of the reading, e.g. "C", "%", "hPa" (optional)
 * @param timestamp  when the reading was taken on the device; defaults to ingestion time when absent
 */
public record Signal(
        @NotBlank String deviceId,
        @NotNull UUID locationId,
        @NotNull SignalType type,
        @NotNull Double value,
        String unit,
        @JsonFormat(shape = JsonFormat.Shape.STRING) Instant timestamp
) {
    /** Returns a copy with {@code timestamp} filled in from {@code fallback} when the client omitted it. */
    public Signal withTimestampOrDefault(Instant fallback) {
        return timestamp != null ? this : new Signal(deviceId, locationId, type, value, unit, fallback);
    }
}
