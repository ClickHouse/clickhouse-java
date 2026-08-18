package com.clickhouse.client.api.observability;

/**
 * Attribute keys recorded on {@link Span}s by the client.
 * <p>
 * Keys follow the OpenTelemetry semantic conventions for database and HTTP client spans. They are
 * defined here, on the SPI side, so that every {@link SpanRecorder} implementation reports the same
 * key for the same piece of information.
 */
public enum SpanAttribute {

    /**
     * Database system name. Always {@code clickhouse}.
     */
    DB_SYSTEM_NAME("db.system.name"),

    /**
     * Target database name.
     */
    DB_NAMESPACE("db.namespace"),

    /**
     * Text of the statement sent to the server. Recorded for query and command operations.
     */
    DB_QUERY_TEXT("db.query.text"),

    /**
     * Table the operation targets. Recorded for insert and table-schema operations.
     */
    DB_COLLECTION_NAME("db.collection.name"),

    /**
     * Name of the client operation. Recorded when the operation is not a plain query - for example
     * {@code insert}, {@code ping} or {@code getTableSchema}.
     */
    DB_OPERATION_NAME("db.operation.name"),

    /**
     * Number of items sent in a single batch. Recorded for POJO inserts.
     */
    DB_OPERATION_BATCH_SIZE("db.operation.batch.size"),

    /**
     * Prefix for statement parameter values. The full key is built with {@link #getKey(String)},
     * for example {@code db.query.parameter.id}.
     */
    DB_QUERY_PARAMETER("db.query.parameter"),

    /**
     * ClickHouse error code returned by the server. Recorded when an operation fails.
     */
    DB_RESPONSE_STATUS_CODE("db.response.status_code"),

    /**
     * Number of rows returned by the server. Recorded when an operation succeeds and the server
     * reported a progress summary.
     */
    DB_RESPONSE_RETURNED_ROWS("db.response.returned_rows"),

    /**
     * Query id of the operation, as assigned by the client or by the server.
     */
    CLICKHOUSE_QUERY_ID("clickhouse.query_id"),

    /**
     * Hostname of the server the request is sent to.
     */
    SERVER_ADDRESS("server.address"),

    /**
     * Port of the server the request is sent to.
     */
    SERVER_PORT("server.port"),

    /**
     * HTTP method of a transport request. Always {@code POST}.
     */
    HTTP_REQUEST_METHOD("http.request.method"),

    /**
     * HTTP status code returned for a transport request. Recorded as soon as a response is received,
     * so it is reported for a successful and for a failed request alike.
     */
    HTTP_RESPONSE_STATUS_CODE("http.response.status_code"),

    /**
     * Type of the error that made an operation or a request fail. Set with
     * {@link Span#setError(String)}.
     */
    ERROR_TYPE("error.type");

    private final String key;

    SpanAttribute(String key) {
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

    /**
     * Returns the attribute key for a named member of an attribute family - {@code key.suffix}.
     * Used for {@link #DB_QUERY_PARAMETER}.
     *
     * @param suffix - name of the family member, for example a statement parameter name
     * @return attribute key with the suffix appended
     */
    public String getKey(String suffix) {
        return key + "." + suffix;
    }
}
