package com.clickhouse.examples.config;


import com.clickhouse.client.api.observability.MetricsRecorder;
import com.clickhouse.client.api.observability.SpanRecorder;
import com.clickhouse.client.api.observability.micrometer.MicrometerMetricsRecorder;
import com.clickhouse.client.api.observability.otel.OpenTelemetrySpanRecorder;
import com.clickhouse.examples.telemetry.ClickHouseMetricExporter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;

/**
 * Builds the application OpenTelemetry SDK for custom metrics. Metrics continue to be stored
 * in ClickHouse and, when OTLP export is enabled, are also sent to the configured collector.
 * The Java agent owns HTTP and ClickHouse tracing so both spans share one context.
 */
@Configuration
public class OpenTelemetryConfig {

    @Bean
    public ClickHouseMetricExporter clickHouseMetricExporter(JdbcTemplate jdbc) {
        return new ClickHouseMetricExporter(jdbc);
    }

    @Bean(destroyMethod = "close")
    public OpenTelemetrySdk openTelemetrySdk(
            ClickHouseMetricExporter exporter,
            @Value("${iot.telemetry.export-interval:15s}") Duration exportInterval,
            @Value("${iot.telemetry.otlp.enabled:true}") boolean otlpEnabled,
            @Value("${iot.telemetry.otlp.endpoint:http://localhost:4317}") String otlpEndpoint,
            @Value("${spring.application.name:iot-ingest}") String serviceName) {

        Resource resource = Resource.getDefault().merge(Resource.create(
                Attributes.of(AttributeKey.stringKey("service.name"), serviceName)));

        SdkMeterProviderBuilder meterProviderBuilder = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(exporter)
                        .setInterval(exportInterval)
                        .build());

        if (otlpEnabled) {
            OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(otlpEndpoint)
                    .build();
            meterProviderBuilder.registerMetricReader(PeriodicMetricReader.builder(metricExporter)
                    .setInterval(exportInterval)
                    .build());
        }

        SdkMeterProvider meterProvider = meterProviderBuilder.build();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .build();

        return OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @Bean
    public OpenTelemetry openTelemetry(OpenTelemetrySdk sdk) {
        return sdk;
    }

    /**
     * Lets the ClickHouse client report its own query and insert spans.
     *
     * <p>The spans must be created through the globally registered instance rather than through the
     * SDK above: the agent owns tracing, and the SDK built here has no span processor because it
     * only exports metrics. Without an agent the global instance is a no-op and the client records
     * nothing.
     */
    @Bean
    public SpanRecorder clickHouseSpanRecorder() {
        return new OpenTelemetrySpanRecorder(GlobalOpenTelemetry.get());
    }

    /**
     * Lets the ClickHouse client record operation metrics (duration, serialization, count, retries)
     * using the Micrometer MetricsRecorder SPI.
     */
    @Bean
    public MetricsRecorder clickHouseMetricsRecorder(MeterRegistry meterRegistry) {
        return new MicrometerMetricsRecorder(meterRegistry);
    }
}
