package com.clickhouse.client.api.metrics;

/**
 * Stats returned by the server.
 * <p>
 * `-1` means the value is not available.
 */
public enum ServerMetrics {

    /**
     * Number of rows read by server from the storage.
     */
    NUM_ROWS_READ("server.numRowsRead"),

    /**
     * Number of rows written by server to the storage.
     */
    NUM_ROWS_WRITTEN("server.numRowsWritten"),

    /**
     * Estimated number of rows to read from the storage.
     * <p>
     */
    TOTAL_ROWS_TO_READ("server.totalRowsToRead"),

    /**
     * Number of bytes read by server from the storage.
     */
    NUM_BYTES_READ("server.numBytesRead"),

    /**
     * Number of bytes written by server to the storage.
     */
    NUM_BYTES_WRITTEN("server.numBytesWritten"),

    /**
     * Number of returned rows.
     */
    RESULT_ROWS("server.resultRows"),

    /**
     * Elapsed time in nanoseconds.
     */
    ELAPSED_TIME("server.elapsedTime");

    private final String key;

    ServerMetrics(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
