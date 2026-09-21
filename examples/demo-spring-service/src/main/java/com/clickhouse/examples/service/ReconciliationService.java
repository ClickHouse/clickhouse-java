package com.clickhouse.examples.service;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertResponse;
import com.clickhouse.examples.model.ReconciliationSignal;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Persists reconciliation batches through the direct ClickHouse Client V2 API.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final Client client;

    public ReconciliationService(Client client) {
        this.client = client;
    }

    /**
     * Runs after the controller has handed the batch to Spring's task executor.
     *
     * <p>The captured request context is restored on the worker, so the span the client reports for
     * the insert remains a child of the HTTP span even though the response is already complete.
     */
    @Async("reconciliationExecutor")
    public void reconcile(List<ReconciliationSignal> signals, Context requestContext) {
        try (Scope ignored = requestContext.makeCurrent()) {
            try (InsertResponse response = client.insert("iot_signals", signals).get()) {
                log.debug(
                        "Reconciled batch size={} writtenRows={} queryId={}",
                        signals.size(),
                        response.getWrittenRows(),
                        response.getQueryId());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.error("Reconciliation interrupted for batch size={}", signals.size(), ex);
            } catch (ExecutionException | RuntimeException ex) {
                log.error("Reconciliation failed for batch size={}", signals.size(), ex);
            }
        }
    }
}
