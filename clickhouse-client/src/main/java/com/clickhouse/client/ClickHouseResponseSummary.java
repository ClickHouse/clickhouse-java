package com.clickhouse.client;

import java.io.Serializable;

/**
 * Summary of ClickHouse response.
 */
public interface ClickHouseResponseSummary extends Serializable {
    default long getAllocatedBytes() {
        return 0L;
    }

    default long getBlocks() {
        return 0L;
    }

    default long getRows() {
        return 0L;
    }

    default long getRowsBeforeLimit() {
        return 0L;
    }

    default long getReadBytes() {
        return 0L;
    }

    default long getReadRows() {
        return 0L;
    }

    default long getTotalRowsToRead() {
        return 0L;
    }

    default long getWriteBytes() {
        return 0L;
    }

    default long getWriteRows() {
        return 0L;
    }
}
