package com.clickhouse.examples.web;

import com.clickhouse.examples.model.Signal;
import com.clickhouse.examples.model.SignalEntity;
import com.clickhouse.examples.repository.SignalRepository;
import com.clickhouse.examples.telemetry.SignalMetrics;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Ingestion endpoint for IoT signals. All routes here are guarded by {@link ApiKeyAuthFilter}.
 */
@RestController
@RequestMapping("/api/v1/signals")
public class SignalController {

    private static final Logger log = LoggerFactory.getLogger(SignalController.class);

    private final SignalRepository repository;
    private final SignalMetrics metrics;
    private final Clock clock;

    @Autowired
    public SignalController(SignalRepository repository, SignalMetrics metrics) {
        this(repository, metrics, Clock.systemUTC());
    }

    SignalController(SignalRepository repository, SignalMetrics metrics, Clock clock) {
        this.repository = repository;
        this.metrics = metrics;
        this.clock = clock;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> ingest(@Valid @RequestBody Signal signal) {
        Signal stored = signal.withTimestampOrDefault(clock.instant());
        measureStorage("insert", () -> repository.save(SignalEntity.from(stored)));
        metrics.recordReceived(stored.type());
        log.debug("Stored signal type={} device={}", stored.type(), stored.deviceId());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "accepted", "type", stored.type().name()));
    }

    @GetMapping("/count")
    public Map<String, Long> count() {
        return Map.of("count", measureStorage("count", repository::count));
    }

    private <T> T measureStorage(String operationName, Supplier<T> operation) {
        long startNanos = System.nanoTime();
        boolean success = false;
        try {
            T result = operation.get();
            success = true;
            return result;
        } finally {
            metrics.recordStorageOperation(
                    operationName,
                    success,
                    Duration.ofNanos(System.nanoTime() - startNanos));
        }
    }
}
