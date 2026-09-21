package com.clickhouse.examples;

import com.clickhouse.client.api.observability.MetricsRecorder;
import com.clickhouse.client.api.observability.micrometer.MicrometerMetricsRecorder;
import com.clickhouse.examples.repository.SignalRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end ingestion test running against a real ClickHouse container.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "iot.telemetry.otlp.enabled=false",
            "management.otlp.metrics.export.enabled=false"
        })
@Import(TestcontainersConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class SignalIngestionTests {

    @Autowired
    TestRestTemplate rest;

    @Autowired
    SignalRepository repository;

    @Autowired
    OpenTelemetrySdk openTelemetry;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    MetricsRecorder metricsRecorder;

    @Test
    void configuresMicrometerMetricsRecorder() {
        assertThat(metricsRecorder).isInstanceOf(MicrometerMetricsRecorder.class);
    }

    @Test
    void registersClickHouseHttpConnectionPoolMetrics() {
        assertThat(meterRegistry
                        .find("httpcomponents.httpclient.pool.total.connections")
                        .tags("httpclient", "reconciliation", "state", "leased")
                        .gauge())
                .isNotNull();
        assertThat(meterRegistry
                        .find("httpcomponents.httpclient.pool.total.pending")
                        .tag("httpclient", "reconciliation")
                        .gauge())
                .isNotNull();
        assertThat(meterRegistry
                        .find("httpcomponents.httpclient.pool.total.max")
                        .tag("httpclient", "reconciliation")
                        .gauge())
                .isNotNull()
                .extracting(gauge -> gauge.value())
                .isEqualTo(10.0);
    }

    @Test
    void rejectsRequestsWithoutApiKey() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(
                "{\"deviceId\":\"sensor-1\",\"type\":\"TEMPERATURE\",\"value\":21.5}", headers);

        ResponseEntity<String> response = rest.postForEntity("/api/v1/signals", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void acceptsAuthenticatedSignalAndStoresIt() {
        long before = repository.count();
        UUID locationId = UUID.randomUUID();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "dev-key-1");
        HttpEntity<String> request = new HttpEntity<>(
                """
                {"deviceId":"sensor-1","locationId":"%s","type":"TEMPERATURE","value":21.5,"unit":"C"}
                """.formatted(locationId),
                headers);

        ResponseEntity<Map<String, Object>> response = rest.exchange(
                "/api/v1/signals",
                HttpMethod.POST,
                request,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).containsEntry("status", "accepted");
        assertThat(repository.count()).isEqualTo(before + 1);

        assertThat(openTelemetry.getSdkMeterProvider().forceFlush().join(10, TimeUnit.SECONDS).isSuccess())
                .isTrue();
        Double exportedCount = jdbc.queryForObject("""
                SELECT max(value)
                FROM otel_metrics
                WHERE name = 'iot.signals.received'
                  AND attributes['signal.type'] = 'TEMPERATURE'
                """, Double.class);
        assertThat(exportedCount).isNotNull().isGreaterThanOrEqualTo(1.0);

        Double storageWrites = jdbc.queryForObject("""
                SELECT max(value)
                FROM otel_metrics
                WHERE name = 'iot.storage.operations'
                  AND attributes['storage.operation'] = 'insert'
                  AND attributes['outcome'] = 'success'
                """, Double.class);
        assertThat(storageWrites).isNotNull().isGreaterThanOrEqualTo(1.0);

        Double durationSamples = jdbc.queryForObject("""
                SELECT max(value)
                FROM otel_metrics
                WHERE name = 'iot.storage.duration'
                  AND type = 'histogram_count'
                  AND attributes['storage.operation'] = 'insert'
                  AND attributes['outcome'] = 'success'
                """, Double.class);
        assertThat(durationSamples).isNotNull().isGreaterThanOrEqualTo(1.0);
    }

    @Test
    void acceptsReconciliationBatchAndStoresItAsynchronously() throws InterruptedException {
        UUID firstId = UUID.randomUUID();
        UUID secondId = UUID.randomUUID();
        UUID locationId = UUID.randomUUID();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "dev-key-1");
        HttpEntity<String> request = new HttpEntity<>("""
                [
                  {
                    "signalId": "%s",
                    "deviceId": "reconciliation-1",
                    "locationId": "%s",
                    "signalType": "TEMPERATURE",
                    "value": 20.5,
                    "unit": "C"
                  },
                  {
                    "signalId": "%s",
                    "deviceId": "reconciliation-2",
                    "locationId": "%s",
                    "signalType": "HUMIDITY",
                    "value": 51.0,
                    "unit": "%%"
                  }
                ]
                """.formatted(firstId, locationId, secondId, locationId), headers);

        ResponseEntity<Map<String, Object>> response = rest.exchange(
                "/api/v1/reconciliation",
                HttpMethod.POST,
                request,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody())
                .containsEntry("status", "accepted")
                .containsEntry("count", 2);
        awaitReconciledSignals(firstId, secondId);

        assertThat(meterRegistry
                        .find("db.client.operation.duration")
                        .tag("db.system.name", "clickhouse")
                        .tag("db.operation.name", "insert")
                        .tag("db.collection.name", "iot_signals")
                        .timer())
                .isNotNull()
                .satisfies(timer -> assertThat(timer.count()).isGreaterThanOrEqualTo(1));
        assertThat(meterRegistry
                        .find("clickhouse.client.operation.count")
                        .tag("db.system.name", "clickhouse")
                        .tag("db.operation.name", "insert")
                        .tag("db.collection.name", "iot_signals")
                        .counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.count()).isGreaterThanOrEqualTo(1.0));
    }

    @Test
    void returnsOneSecondMeanRowsFilteredByLocation() {
        UUID locationId = UUID.randomUUID();
        Instant eventTime = Instant.now()
                .minusSeconds(1)
                .truncatedTo(ChronoUnit.SECONDS)
                .plusMillis(100);

        postSignal(locationId, "slice-device-1", "TEMPERATURE", 20.0, "C", eventTime);
        postSignal(locationId, "slice-device-2", "TEMPERATURE", 24.0, "C", eventTime.plusMillis(100));
        postSignal(locationId, "slice-device-3", "HUMIDITY", 50.0, "%", eventTime.plusMillis(200));

        ResponseEntity<java.util.List<Map<String, Object>>> response = rest.exchange(
                "/api/v1/signals/slices?lookback=10s&locationId=" + locationId,
                HttpMethod.GET,
                new HttpEntity<>(authenticatedHeaders()),
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).singleElement().satisfies(row -> {
            assertThat(row).containsEntry("locationId", locationId.toString());
            assertThat(((Number) row.get("temperature")).doubleValue()).isEqualTo(22.0);
            assertThat(((Number) row.get("humidity")).doubleValue()).isEqualTo(50.0);
            assertThat(row.get("pressure")).isNull();
        });

        assertThat(meterRegistry
                        .find("db.client.operation.duration")
                        .tag("db.system.name", "clickhouse")
                        .tag("db.operation.name", "query")
                        .timer())
                .isNotNull()
                .satisfies(timer -> assertThat(timer.count()).isGreaterThanOrEqualTo(1));
        assertThat(meterRegistry
                        .find("clickhouse.client.operation.count")
                        .tag("db.system.name", "clickhouse")
                        .tag("db.operation.name", "query")
                        .counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.count()).isGreaterThanOrEqualTo(1.0));
    }

    @Test
    void rejectsSliceLookbackLongerThanOneHour() {
        ResponseEntity<String> response = rest.exchange(
                "/api/v1/signals/slices?lookback=61m",
                HttpMethod.GET,
                new HttpEntity<>(authenticatedHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void rejectsUnknownSignalType() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "dev-key-1");
        HttpEntity<String> request = new HttpEntity<>(
                "{\"deviceId\":\"sensor-1\",\"type\":\"PLASMA\",\"value\":1.0}", headers);

        ResponseEntity<String> response = rest.postForEntity("/api/v1/signals", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void rejectsSignalWithoutValue() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "dev-key-1");
        HttpEntity<String> request = new HttpEntity<>(
                "{\"deviceId\":\"sensor-1\",\"type\":\"TEMPERATURE\"}", headers);

        ResponseEntity<String> response = rest.postForEntity("/api/v1/signals", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private void postSignal(
            UUID locationId,
            String deviceId,
            String type,
            double value,
            String unit,
            Instant eventTime) {
        HttpHeaders headers = authenticatedHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>("""
                {
                  "deviceId": "%s",
                  "locationId": "%s",
                  "type": "%s",
                  "value": %s,
                  "unit": "%s",
                  "timestamp": "%s"
                }
                """.formatted(deviceId, locationId, type, value, unit, eventTime), headers);

        ResponseEntity<String> response = rest.postForEntity("/api/v1/signals", request, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    private HttpHeaders authenticatedHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-API-Key", "dev-key-1");
        return headers;
    }

    private void awaitReconciledSignals(UUID firstId, UUID secondId) throws InterruptedException {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        long stored;
        do {
            stored = jdbc.queryForObject(
                    "SELECT count() FROM iot_signals WHERE signal_id IN (?, ?)",
                    Long.class,
                    firstId,
                    secondId);
            if (stored == 2) {
                return;
            }
            Thread.sleep(50);
        } while (Instant.now().isBefore(deadline));

        assertThat(stored).as("asynchronously reconciled signals").isEqualTo(2);
    }
}
