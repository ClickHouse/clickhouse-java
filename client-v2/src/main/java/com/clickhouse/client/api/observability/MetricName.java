package com.clickhouse.client.api.observability;

/**
 * Metrics reported by the client, with the name, the unit and the description an exporter should
 * register its instrument with.
 * <p>
 * Names and units follow the OpenTelemetry semantic conventions for database clients where a
 * convention exists, and are placed under {@code clickhouse.} where it does not - the same rule
 * {@link SpanAttribute} follows. Units are the UCUM codes the conventions use, so a duration is
 * reported in <b>seconds</b> (the values of {@link com.clickhouse.client.api.metrics.ClientMetrics}
 * are milliseconds; {@link MetricsSupport} converts them).
 * <p>
 * The names are defined here, on the SPI side, so that every {@link MetricsRecorder} implementation
 * reports the same metric under the same name.
 */
public enum MetricName {

    /**
     * Duration of a client operation. Reported for a successful and for a failed operation alike, so
     * the number of operations by outcome is the count of this metric grouped by
     * {@link MetricAttribute#ERROR_TYPE}.
     */
    OPERATION_DURATION("db.client.operation.duration", "s", "Duration of a ClickHouse client operation."),

    /**
     * Duration of the serialization step of a client operation. Reported when the client measured it,
     * which it does for an insert of POJOs.
     */
    OPERATION_SERIALIZATION_DURATION("clickhouse.client.operation.serialization.duration", "s",
            "Duration of the serialization step of a ClickHouse client operation."),

    /**
     * Number of completed client operations. Counted by outcome - a successful operation has no
     * {@link MetricAttribute#ERROR_TYPE} attribute, a failed one carries it.
     */
    OPERATION_COUNT("clickhouse.client.operation.count", "{operation}",
            "Number of completed ClickHouse client operations, by outcome."),

    /**
     * Number of retried attempts of client operations. An operation that succeeds on its third
     * attempt contributes two retries.
     */
    OPERATION_RETRIES("clickhouse.client.operation.retries", "{retry}",
            "Number of retried attempts of ClickHouse client operations.");

    private final String key;
    private final String unit;
    private final String description;

    MetricName(String key, String unit, String description) {
        this.key = key;
        this.unit = unit;
        this.description = description;
    }

    /**
     * Returns the name of the metric.
     *
     * @return metric name
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the unit of the metric as a UCUM code - {@code s} for a duration, an annotation like
     * {@code {operation}} for a count.
     *
     * @return metric unit
     */
    public String getUnit() {
        return unit;
    }

    /**
     * Returns the description of the metric.
     *
     * @return metric description
     */
    public String getDescription() {
        return description;
    }
}
