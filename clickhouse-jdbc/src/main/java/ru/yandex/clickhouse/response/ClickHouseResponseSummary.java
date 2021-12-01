package ru.yandex.clickhouse.response;

public class ClickHouseResponseSummary {
    private final long read_rows; // number of read rows for selects (may be more than rows in result set)
    private final long written_rows; // number of written rows for inserts
    private final long read_bytes;
    private final long written_bytes;
    private final long total_rows_to_read;

    public ClickHouseResponseSummary(long read_rows, long written_rows, long read_bytes, long written_bytes,
            long total_rows_to_read) {
        this.read_rows = read_rows;
        this.written_rows = written_rows;
        this.read_bytes = read_bytes;
        this.written_bytes = written_bytes;
        this.total_rows_to_read = total_rows_to_read;
    }

    public long getReadRows() {
        return read_rows;
    }

    public long getWrittenRows() {
        return written_rows;
    }

    public long getReadBytes() {
        return read_bytes;
    }

    public long getWrittenBytes() {
        return written_bytes;
    }

    public long getTotalRowsToRead() {
        return total_rows_to_read;
    }
}
