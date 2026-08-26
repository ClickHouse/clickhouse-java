package com.clickhouse.client.api.metrics;

/**
 * Kind of client operation a set of {@link OperationMetrics} was collected for.
 * <p>
 * The kind decides which metrics of the operation are meaningful: a read operation reports how much
 * the server read and returned, an insert reports how much the server wrote.
 */
public enum OperationType {

    /**
     * Operation the client ran as a statement - a query, a command, a ping or a table-schema lookup.
     * It is the kind of the call the application made, not of the work the server did: a command that
     * writes, such as {@code INSERT INTO ... SELECT}, is run as a statement and is reported here.
     */
    QUERY,

    /**
     * Operation the client ran as an insert, through one of the {@code insert} methods.
     */
    INSERT
}
