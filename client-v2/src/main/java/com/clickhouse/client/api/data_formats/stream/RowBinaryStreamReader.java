package com.clickhouse.client.api.data_formats.stream;

import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Reads {@code RowBinary}, {@code RowBinaryWithNames}, or {@code RowBinaryWithNamesAndTypes}
 * without materializing nested containers.
 * <p>
 * Experimental API. Not thread-safe. The caller owns the input stream. A failure desynchronizes
 * the stream; the reader must not be reused.
 */
public final class RowBinaryStreamReader {

    private final PushbackInputStream stream;

    private final BinaryStreamReader binary;

    private final ClickHouseFormat format;

    private final TimeZone timeZone;

    private final byte[] scratch = new byte[8];

    private final byte[] skipBuffer = new byte[256];

    private TableSchema schema;

    private List<ClickHouseColumn> columns;

    private boolean failed;

    private StreamReaderScope openRow;

    public RowBinaryStreamReader(InputStream in, TableSchema schema, ClickHouseFormat format) throws IOException {
        this(in, schema, format, TimeZone.getTimeZone(ZoneOffset.UTC));
    }

    public RowBinaryStreamReader(InputStream in, TableSchema schema, ClickHouseFormat format, TimeZone timeZone)
            throws IOException {
        if (in == null) {
            throw new IllegalArgumentException("Input stream is required");
        }
        if (format != ClickHouseFormat.RowBinary && format != ClickHouseFormat.RowBinaryWithNames
                && format != ClickHouseFormat.RowBinaryWithNamesAndTypes) {
            throw new IllegalArgumentException(
                    "Only RowBinary, RowBinaryWithNames, and RowBinaryWithNamesAndTypes are supported");
        }
        if (format != ClickHouseFormat.RowBinaryWithNamesAndTypes && schema == null) {
            throw new IllegalArgumentException("Schema is required");
        }
        this.stream = new PushbackInputStream(in, 1);
        this.timeZone = timeZone == null ? TimeZone.getTimeZone(ZoneOffset.UTC) : timeZone;
        this.binary = new BinaryStreamReader(stream, this.timeZone, null,
                new BinaryStreamReader.CachingByteBufferAllocator(), true, null, true);
        this.format = format;
        if (schema != null) {
            this.schema = schema;
            this.columns = new ArrayList<ClickHouseColumn>(schema.getColumns());
        }
        if (format == ClickHouseFormat.RowBinaryWithNames) {
            readNameHeader();
        } else if (format == ClickHouseFormat.RowBinaryWithNamesAndTypes) {
            readNamesAndTypesHeader();
        }
    }

    public boolean hasNext() throws IOException {
        if (failed) {
            throw new IllegalStateException("Reader is unusable after a previous failure");
        }
        if (openRow != null && !openRow.isClosed()) {
            throw new IllegalStateException("A row is still open");
        }
        int next = stream.read();
        if (next < 0) {
            return false;
        }
        stream.unread(next);
        return true;
    }

    public ValueReader beginRow() throws IOException {
        if (failed) {
            throw new IllegalStateException("Reader is unusable after a previous failure");
        }
        if (openRow != null && !openRow.isClosed()) {
            throw new IllegalStateException("A row is still open");
        }
        if (!hasNext()) {
            throw new EOFException("No more rows");
        }
        openRow = StreamReaderScope.row(this, columns);
        return openRow;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public ClickHouseFormat getFormat() {
        return format;
    }

    InputStream stream() {
        return stream;
    }

    BinaryStreamReader binary() {
        return binary;
    }

    TimeZone timeZone() {
        return timeZone;
    }

    byte[] scratch() {
        return scratch;
    }

    boolean isFailed() {
        return failed;
    }

    void fail() {
        failed = true;
    }

    void onRowClosed() {
        openRow = null;
    }

    void unread(int value) throws IOException {
        stream.unread(value);
    }

    void skipFully(int length) throws IOException {
        int left = length;
        while (left > 0) {
            int n = stream.read(skipBuffer, 0, Math.min(skipBuffer.length, left));
            if (n < 0) {
                throw new EOFException("End of stream reached before skipping " + length + " bytes");
            }
            left -= n;
        }
    }

    private void readNameHeader() throws IOException {
        List<String> names = readNames();
        if (names.size() != columns.size()) {
            throw new ClientException("RowBinaryWithNames column count " + names.size()
                    + " does not match schema " + columns.size());
        }
        for (int i = 0; i < names.size(); i++) {
            if (!names.get(i).equals(columns.get(i).getColumnName())) {
                throw new ClientException("RowBinaryWithNames column '" + names.get(i)
                        + "' does not match schema column '" + columns.get(i).getColumnName() + "'");
            }
        }
    }

    private void readNamesAndTypesHeader() throws IOException {
        List<String> names = readNames();
        List<ClickHouseColumn> header = new ArrayList<ClickHouseColumn>(names.size());
        try {
            for (int i = 0; i < names.size(); i++) {
                header.add(ClickHouseColumn.of(names.get(i), binary.readString()));
            }
        } catch (IOException e) {
            throw new ClientException("Failed to read header", e);
        }
        this.columns = header;
        this.schema = new TableSchema(header);
    }

    private List<String> readNames() throws IOException {
        int count;
        try {
            count = BinaryStreamReader.readVarInt(stream);
        } catch (EOFException e) {
            throw new ClientException("Failed to read header", e);
        } catch (IOException e) {
            throw new ClientException("Failed to read header", e);
        }
        List<String> names = new ArrayList<String>(count);
        try {
            for (int i = 0; i < count; i++) {
                names.add(binary.readString());
            }
        } catch (IOException e) {
            throw new ClientException("Failed to read header", e);
        }
        return names;
    }
}
