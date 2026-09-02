package com.clickhouse.examples.config;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.observability.MetricsRecorder;
import com.clickhouse.client.api.observability.SpanRecorder;
import com.clickhouse.examples.model.ReconciliationSignal;
import io.opentelemetry.context.Context;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Configures the direct ClickHouse client and the Spring-owned reconciliation executor.
 */
@Configuration
@EnableAsync
public class ReconciliationConfig {

    @Bean(destroyMethod = "close")
    @DependsOn("clickHouseSchemaInitializer")
    public Client clickHouseClient(
            JdbcConnectionDetails connectionDetails,
            MeterRegistry meterRegistry,
            SpanRecorder spanRecorder,
            MetricsRecorder metricsRecorder,
            @Value("${iot.telemetry.clickhouse-client.metrics-group:reconciliation}") String metricsGroup) {
        ClickHouseAddress address = ClickHouseAddress.fromJdbcUrl(connectionDetails.getJdbcUrl());

        Client client = new Client.Builder()
                .addEndpoint(address.endpoint())
                .setDefaultDatabase(address.database())
                .setUsername(connectionDetails.getUsername())
                .setPassword(connectionDetails.getPassword())
                // Spring owns the asynchronous boundary; the client performs work on that thread.
                // Running on the caller also lets client spans join the trace already in progress.
                .useAsyncRequests(false)
                // Publishes Apache HttpClient 5 connection-pool gauges to Spring's registry.
                .registerClientMetrics(meterRegistry, metricsGroup)
                // Reports a span per query and insert, plus one per HTTP attempt below it.
                .setSpanRecorder(spanRecorder)
                // Records client operation metrics (duration, serialization, count, retries) via Micrometer.
                .setMetricsRecorder(metricsRecorder)
                // Puts the identifier on the span as db.query.id, so a trace can be looked up
                // in ClickHouse's system.query_log.
                .setQueryIdGenerator(() -> UUID.randomUUID().toString())
                .build();

        try {
            client.register(ReconciliationSignal.class, client.getTableSchema("iot_signals"));
            return client;
        } catch (RuntimeException ex) {
            client.close();
            throw ex;
        }
    }

    @Bean
    @Qualifier("reconciliationExecutor")
    public Executor reconciliationExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("reconciliation-");
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(100);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setTaskDecorator(openTelemetryContext());
        return executor;
    }

    private TaskDecorator openTelemetryContext() {
        return task -> Context.current().wrap(task);
    }

    private record ClickHouseAddress(String endpoint, String database) {

        private static ClickHouseAddress fromJdbcUrl(String jdbcUrl) {
            String uriValue;
            if (jdbcUrl.startsWith("jdbc:clickhouse:http://")
                    || jdbcUrl.startsWith("jdbc:clickhouse:https://")) {
                uriValue = jdbcUrl.substring("jdbc:clickhouse:".length());
            } else if (jdbcUrl.startsWith("jdbc:clickhouse://")) {
                uriValue = "http:" + jdbcUrl.substring("jdbc:clickhouse:".length());
            } else if (jdbcUrl.startsWith("jdbc:ch://")) {
                uriValue = "http:" + jdbcUrl.substring("jdbc:ch:".length());
            } else {
                throw new IllegalArgumentException("Unsupported ClickHouse JDBC URL: " + jdbcUrl);
            }

            URI uri = URI.create(uriValue);
            String path = uri.getPath();
            String database = path == null || path.length() <= 1
                    ? "default"
                    : path.substring(1).split("/", 2)[0];
            String endpoint = uri.getScheme() + "://" + uri.getRawAuthority();
            return new ClickHouseAddress(endpoint, database);
        }
    }
}
