package com.clickhouse.examples.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.UuidGenerator;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA persistence model for the {@code iot_signals} ClickHouse table.
 *
 * <p>The HTTP model remains separate so persistence details do not leak into the API.
 */
@Entity
@Table(name = "iot_signals")
public class SignalEntity {

    @Id
    @GeneratedValue
    @UuidGenerator
    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(name = "signal_id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "device_id", nullable = false)
    private String deviceId;

    @JdbcTypeCode(SqlTypes.VARCHAR)
    @Column(name = "location_id", nullable = false)
    private UUID locationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "signal_type", nullable = false)
    private SignalType type;

    @Column(name = "value", nullable = false)
    private double value;

    @Column(name = "unit", nullable = false)
    private String unit;

    @Column(name = "event_time", nullable = false)
    private Instant eventTime;

    protected SignalEntity() {
        // Required by JPA.
    }

    private SignalEntity(
            String deviceId,
            UUID locationId,
            SignalType type,
            double value,
            String unit,
            Instant eventTime) {
        this.deviceId = deviceId;
        this.locationId = locationId;
        this.type = type;
        this.value = value;
        this.unit = unit;
        this.eventTime = eventTime;
    }

    public static SignalEntity from(Signal signal) {
        return new SignalEntity(
                signal.deviceId(),
                signal.locationId(),
                signal.type(),
                signal.value(),
                signal.unit() == null ? "" : signal.unit(),
                signal.timestamp());
    }

    public UUID getId() {
        return id;
    }
}
