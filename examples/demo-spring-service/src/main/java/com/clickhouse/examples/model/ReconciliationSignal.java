package com.clickhouse.examples.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * JavaBean used by both Jackson request binding and ClickHouse Client V2 POJO serialization.
 */
public class ReconciliationSignal {

    private UUID signalId;

    @NotBlank
    private String deviceId;

    @NotNull
    private UUID locationId;

    @NotNull
    private SignalType signalType;

    @NotNull
    private Double value;

    private String unit;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant eventTime;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant receivedAt;

    public ReconciliationSignal() {
        // Required by Jackson and the ClickHouse POJO serializer.
    }

    public ReconciliationSignal(
            UUID signalId,
            String deviceId,
            UUID locationId,
            SignalType signalType,
            Double value,
            String unit,
            Instant eventTime,
            Instant receivedAt) {
        this.signalId = signalId;
        this.deviceId = deviceId;
        this.locationId = locationId;
        this.signalType = signalType;
        this.value = value;
        this.unit = unit;
        this.eventTime = eventTime;
        this.receivedAt = receivedAt;
    }

    public ReconciliationSignal withDefaults(Instant fallbackTime) {
        return new ReconciliationSignal(
                signalId == null ? UUID.randomUUID() : signalId,
                deviceId,
                locationId,
                signalType,
                value,
                unit == null ? "" : unit,
                eventTime == null ? fallbackTime : eventTime,
                receivedAt == null ? fallbackTime : receivedAt);
    }

    public UUID getSignalId() {
        return signalId;
    }

    public void setSignalId(UUID signalId) {
        this.signalId = signalId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public UUID getLocationId() {
        return locationId;
    }

    public void setLocationId(UUID locationId) {
        this.locationId = locationId;
    }

    public SignalType getSignalType() {
        return signalType;
    }

    public void setSignalType(SignalType signalType) {
        this.signalType = signalType;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }
}
