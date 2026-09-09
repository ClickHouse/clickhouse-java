package com.clickhouse.client.api.observability;

/**
 * Attribute keys recorded on the metrics of {@link MetricName} by the client.
 * <p>
 * Keys follow the OpenTelemetry semantic conventions for database clients. They are defined here, on
 * the SPI side, so that every {@link MetricsRecorder} implementation reports the same key for the
 * same piece of information.
 * <p>
 * This is deliberately a smaller set than {@link SpanAttribute}: an attribute of a metric becomes a
 * time series, so only low-cardinality values are reported. The statement text, the query id and the
 * statement parameters are recorded on spans only.
 */
public enum MetricAttribute {

    /**
     * Database system name. Always {@code clickhouse}.
     */
    DB_SYSTEM_NAME("db.system.name"),

    /**
     * Target database name.
     */
    DB_NAMESPACE("db.namespace"),

    /**
     * Name of the client operation - {@code query} or {@code insert}.
     */
    DB_OPERATION_NAME("db.operation.name"),

    /**
     * Table the operation targets. Recorded for an insert.
     */
    DB_COLLECTION_NAME("db.collection.name"),

    /**
     * ClickHouse error code returned by the server. Recorded when an operation fails and the server
     * reported one.
     */
    DB_RESPONSE_STATUS_CODE("db.response.status_code"),

    /**
     * Type of the error that made an operation fail, usually an exception class name. Recorded only
     * on a failure, so a time series without it is the successful one.
     */
    ERROR_TYPE("error.type");

    private final String key;

    MetricAttribute(String key) {
        this.key = key;
    }

    /**
     * Returns the attribute key.
     *
     * @return attribute key
     */
    public String getKey() {
        return key;
    }
}
