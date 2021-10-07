package com.clickhouse.client.grpc;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import com.clickhouse.client.ClickHouseResponseSummary;

public class ClickHouseGrpcResponseSummary implements ClickHouseResponseSummary {
    // grpc
    protected final AtomicInteger results;

    // stats
    protected final AtomicLong allocatedBytes;
    protected final AtomicLong blocks;
    protected final AtomicLong rows;
    protected final AtomicLong rowsBeforeLimit;

    // progress
    protected final AtomicLong readBytes;
    protected final AtomicLong readRows;
    protected final AtomicLong totalRowsToRead;
    protected final AtomicLong writeBytes;
    protected final AtomicLong writeRows;

    protected ClickHouseGrpcResponseSummary() {
        this.results = new AtomicInteger(0);

        this.readBytes = new AtomicLong(0L);
        this.readRows = new AtomicLong(0L);
        this.totalRowsToRead = new AtomicLong(0L);
        this.writeBytes = new AtomicLong(0L);
        this.writeRows = new AtomicLong(0L);

        this.allocatedBytes = new AtomicLong(0L);
        this.blocks = new AtomicLong(0L);
        this.rows = new AtomicLong(0L);
        this.rowsBeforeLimit = new AtomicLong(0L);
    }

    public int getResults() {
        return results.get();
    }

    @Override
    public long getAllocatedBytes() {
        return allocatedBytes.get();
    }

    @Override
    public long getBlocks() {
        return blocks.get();
    }

    @Override
    public long getRows() {
        return rows.get();
    }

    @Override
    public long getRowsBeforeLimit() {
        return rowsBeforeLimit.get();
    }

    @Override
    public long getReadBytes() {
        return readBytes.get();
    }

    @Override
    public long getReadRows() {
        return readRows.get();
    }

    @Override
    public long getTotalRowsToRead() {
        return totalRowsToRead.get();
    }

    @Override
    public long getWriteBytes() {
        return writeBytes.get();
    }

    @Override
    public long getWriteRows() {
        return writeRows.get();
    }
}
