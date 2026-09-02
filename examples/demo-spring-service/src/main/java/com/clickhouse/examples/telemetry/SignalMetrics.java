package com.clickhouse.examples.telemetry;

import com.clickhouse.examples.model.SignalType;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Application metrics recorded through the OpenTelemetry API.
 *
 * <p>These instruments feed the configured metric reader, which exports the aggregated
 * values to ClickHouse (see {@link ClickHouseMetricExporter}). The primary metric is the
 * number of signals received, broken down by {@code signal.type}.
 */
@Component
public class SignalMetrics {

    private static final AttributeKey<String> SIGNAL_TYPE = AttributeKey.stringKey("signal.type");
    private static final AttributeKey<String> STORAGE_OPERATION = AttributeKey.stringKey("storage.operation");
    private static final AttributeKey<String> OUTCOME = AttributeKey.stringKey("outcome");

    private final LongCounter signalsReceived;
    private final LongCounter signalsRejected;
    private final LongCounter authFailures;
    private final LongCounter storageOperations;
    private final DoubleHistogram storageDuration;

    public SignalMetrics(OpenTelemetry openTelemetry) {
        Meter meter = openTelemetry.getMeter("com.clickhouse.examples");

        this.signalsReceived = meter.counterBuilder("iot.signals.received")
                .setDescription("Number of IoT signals accepted and stored, by signal type")
                .setUnit("{signal}")
                .build();
        this.signalsRejected = meter.counterBuilder("iot.signals.rejected")
                .setDescription("Number of IoT signals rejected as invalid, by signal type")
                .setUnit("{signal}")
                .build();
        this.authFailures = meter.counterBuilder("iot.auth.failures")
                .setDescription("Number of requests rejected due to a missing or invalid API key")
                .setUnit("{request}")
                .build();
        this.storageOperations = meter.counterBuilder("iot.storage.operations")
                .setDescription("Number of JPA/ClickHouse operations by operation and outcome")
                .setUnit("{operation}")
                .build();
        this.storageDuration = meter.histogramBuilder("iot.storage.duration")
                .setDescription("JPA/ClickHouse operation duration by operation and outcome")
                .setUnit("ms")
                .build();
    }

    public void recordReceived(SignalType type) {
        signalsReceived.add(1, Attributes.of(SIGNAL_TYPE, type.name()));
    }

    public void recordRejected(SignalType type) {
        String label = type == null ? "UNKNOWN" : type.name();
        signalsRejected.add(1, Attributes.of(SIGNAL_TYPE, label));
    }

    public void recordAuthFailure() {
        authFailures.add(1);
    }

    public void recordStorageOperation(String operation, boolean success, Duration duration) {
        Attributes attributes = Attributes.of(
                STORAGE_OPERATION, operation,
                OUTCOME, success ? "success" : "failure");
        storageOperations.add(1, attributes);
        storageDuration.record(duration.toNanos() / 1_000_000.0, attributes);
    }
}
