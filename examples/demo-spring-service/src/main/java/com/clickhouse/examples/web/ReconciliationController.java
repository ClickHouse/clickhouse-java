package com.clickhouse.examples.web;

import com.clickhouse.examples.model.ReconciliationSignal;
import com.clickhouse.examples.service.ReconciliationService;
import io.opentelemetry.context.Context;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Accepts batches for fire-and-forget reconciliation through the direct ClickHouse client.
 */
@RestController
@RequestMapping("/api/v1/reconciliation")
public class ReconciliationController {

    private final ReconciliationService reconciliationService;

    public ReconciliationController(ReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @PostMapping
    public DeferredResult<ResponseEntity<Map<String, Object>>> reconcile(
            @Valid @NotEmpty @RequestBody List<@Valid ReconciliationSignal> signals) {
        Instant receivedAt = Instant.now();
        List<ReconciliationSignal> batch = signals.stream()
                .map(signal -> signal.withDefaults(receivedAt))
                .toList();
        Context requestContext = Context.current();

        DeferredResult<ResponseEntity<Map<String, Object>>> result = new DeferredResult<>();
        result.onCompletion(() -> reconciliationService.reconcile(batch, requestContext));
        result.setResult(ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "accepted", "count", batch.size())));

        return result;
    }
}
