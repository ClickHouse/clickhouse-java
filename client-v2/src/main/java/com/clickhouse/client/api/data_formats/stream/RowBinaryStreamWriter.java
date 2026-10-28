package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Writes {@code RowBinary} or {@code RowBinaryWithDefaults} directly to an output stream.
 * Nested values are scopes: a known container size is written through, an unknown size is buffered
 * until the scope is closed.
 * <p>
 * Experimental API. Not thread-safe. The caller owns {@code out}. A failure desynchronizes the
 * stream; the writer must not be reused. Retry by writing the rows again to a new stream.
 */
public final class RowBinaryStreamWriter {

    private final OutputStream out;

    private final List<ClickHouseColumn> columns;

    private final boolean defaultsSupport;

    private final ClickHouseFormat format;

    private final TimeZone timeZone;

    private int rowCount;

    private boolean failed;

    private StreamWriterScope openRow;

    public RowBinaryStreamWriter(OutputStream out, TableSchema schema, ClickHouseFormat format) {
        this(out, schema, format, TimeZone.getTimeZone(ZoneOffset.UTC));
    }

    public RowBinaryStreamWriter(OutputStream out, TableSchema schema, ClickHouseFormat format, TimeZone timeZone) {
        if (out == null) {
            throw new IllegalArgumentException("Output stream is required");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema is required");
        }
        if (format != ClickHouseFormat.RowBinary && format != ClickHouseFormat.RowBinaryWithDefaults) {
            throw new IllegalArgumentException("Only RowBinary and RowBinaryWithDefaults are supported");
        }
        this.out = out;
        this.columns = writableColumns(schema);
        this.defaultsSupport = format == ClickHouseFormat.RowBinaryWithDefaults;
        this.format = format;
        this.timeZone = timeZone == null ? TimeZone.getTimeZone(ZoneOffset.UTC) : timeZone;
    }

    public ValueWriter beginRow() throws IOException {
        if (failed) {
            throw new IllegalStateException("Writer is unusable after a previous failure");
        }
        if (openRow != null && !openRow.isClosed()) {
            throw new IllegalStateException("A row is still open");
        }
        openRow = StreamWriterScope.row(this, columns);
        return openRow;
    }

    public int getRowCount() {
        return rowCount;
    }

    public ClickHouseFormat getFormat() {
        return format;
    }

    OutputStream out() {
        return out;
    }

    boolean defaultsSupport() {
        return defaultsSupport;
    }

    TimeZone timeZone() {
        return timeZone;
    }

    boolean isFailed() {
        return failed;
    }

    void fail() {
        failed = true;
    }

    void onRowClosed() {
        openRow = null;
        rowCount++;
    }

    private static List<ClickHouseColumn> writableColumns(TableSchema schema) {
        List<ClickHouseColumn> all = schema.getColumns();
        List<ClickHouseColumn> kept = new ArrayList<ClickHouseColumn>(all.size());
        for (int i = 0; i < all.size(); i++) {
            ClickHouseColumn column = all.get(i);
            if (!StreamTypes.omitted(column)) {
                kept.add(column);
            }
        }
        return kept;
    }
}
