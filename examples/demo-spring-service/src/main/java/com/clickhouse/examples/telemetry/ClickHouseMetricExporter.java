package com.clickhouse.examples.telemetry;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An OpenTelemetry {@link MetricExporter} that writes metric points into a ClickHouse table.
 *
 * <p>Each exported point becomes one row in {@code otel_metrics}, keeping the metric name,
 * its attributes (e.g. {@code signal.type}) and the aggregated value. This lets the same
 * ClickHouse instance that stores raw signals also serve as the metrics backend, without
 * requiring a separate OpenTelemetry Collector.
 */
public class ClickHouseMetricExporter implements MetricExporter {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseMetricExporter.class);

    private static final String INSERT_SQL = """
            INSERT INTO otel_metrics
                (name, description, unit, type, value, attributes, start_time, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;

    public ClickHouseMetricExporter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Counters report cumulative totals, which is what we want to persist as a time series. */
    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return AggregationTemporality.CUMULATIVE;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        try {
            for (MetricData metric : metrics) {
                switch (metric.getType()) {
                    case LONG_SUM -> exportLong(metric, metric.getLongSumData().getPoints(), "sum");
                    case DOUBLE_SUM -> exportDouble(metric, metric.getDoubleSumData().getPoints(), "sum");
                    case LONG_GAUGE -> exportLong(metric, metric.getLongGaugeData().getPoints(), "gauge");
                    case DOUBLE_GAUGE -> exportDouble(metric, metric.getDoubleGaugeData().getPoints(), "gauge");
                    case HISTOGRAM -> exportHistogram(metric, metric.getHistogramData().getPoints());
                    default -> log.debug("Skipping unsupported metric type {} for {}", metric.getType(), metric.getName());
                }
            }
            return CompletableResultCode.ofSuccess();
        } catch (RuntimeException e) {
            log.warn("Failed to export metrics to ClickHouse", e);
            return CompletableResultCode.ofFailure();
        }
    }

    private void exportLong(MetricData metric, Collection<LongPointData> points, String type) {
        for (LongPointData p : points) {
            insert(metric, type, (double) p.getValue(), p);
        }
    }

    private void exportDouble(MetricData metric, Collection<DoublePointData> points, String type) {
        for (DoublePointData p : points) {
            insert(metric, type, p.getValue(), p);
        }
    }

    private void exportHistogram(MetricData metric, Collection<HistogramPointData> points) {
        for (HistogramPointData p : points) {
            insert(metric, "histogram_count", p.getCount(), p);
            insert(metric, "histogram_sum", p.getSum(), p);
        }
    }

    private void insert(MetricData metric, String type, double value, PointData point) {
        jdbc.update(INSERT_SQL,
                metric.getName(),
                metric.getDescription(),
                metric.getUnit(),
                type,
                value,
                attributesOf(point),
                nanosToInstant(point.getStartEpochNanos()),
                nanosToInstant(point.getEpochNanos()));
    }

    /** Point attributes as a plain {@link Map}; the ClickHouse driver maps this to Map(String,String). */
    private Map<String, String> attributesOf(PointData point) {
        Map<String, String> attrs = new LinkedHashMap<>();
        point.getAttributes().forEach((key, val) -> attrs.put(key.getKey(), String.valueOf(val)));
        return attrs;
    }

    private static Instant nanosToInstant(long epochNanos) {
        return Instant.ofEpochSecond(epochNanos / 1_000_000_000L, epochNanos % 1_000_000_000L);
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }
}
