package ru.yandex.clickhouse.response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClickHouseResponseSummary {
    final private long readRows; // number of read rows for selects (may be more than rows in result set)
    final private long writtenRows; // number of written rows for inserts
    final private long readBytes;
    final private long writtenBytes;
    final private long totalRowsToRead;

    public ClickHouseResponseSummary(@JsonProperty("read_rows") long readRows, @JsonProperty("written_rows") long writtenRows, @JsonProperty("read_bytes") long readBytes,
                                     @JsonProperty("written_bytes") long writtenBytes, @JsonProperty("total_rows_to_read") long totalRowsToRead) {
        this.readRows = readRows;
        this.writtenRows = writtenRows;
        this.readBytes = readBytes;
        this.writtenBytes = writtenBytes;
        this.totalRowsToRead = totalRowsToRead;
    }

    public long getReadRows() {
        return readRows;
    }

    public long getWrittenRows() {
        return writtenRows;
    }

    public long getReadBytes() {
        return readBytes;
    }

    public long getWrittenBytes() {
        return writtenBytes;
    }

    public long getTotalRowsToRead() {
        return totalRowsToRead;
    }
}
