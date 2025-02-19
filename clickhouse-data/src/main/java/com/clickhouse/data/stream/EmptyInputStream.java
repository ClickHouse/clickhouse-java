package com.clickhouse.data.stream;

import java.io.EOFException;
import java.io.IOException;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;

/**
 * Empty input stream produces nothing and it can never be closed.
 */
@Deprecated
public final class EmptyInputStream extends ClickHouseInputStream {
    public static final EmptyInputStream INSTANCE = new EmptyInputStream();

    private EmptyInputStream() {
        super(null, null, null);
    }

    @Override
    protected ClickHouseByteBuffer getBuffer() {
        return byteBuffer.reset();
    }

    @Override
    protected ClickHouseByteBuffer nextBuffer() throws IOException {
        return byteBuffer.reset();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public int peek() throws IOException {
        return -1;
    }

    @Override
    public long pipe(ClickHouseOutputStream output) throws IOException {
        return 0L;
    }

    @Override
    public byte readByte() throws IOException {
        throw new EOFException();
    }

    @Override
    public ClickHouseByteBuffer readBufferUntil(byte[] separator) throws IOException {
        return byteBuffer.reset();
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        return getBuffer();
    }

    @Override
    public int read() throws IOException {
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return len < 1 ? 0 : -1;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public int available() throws IOException {
        return 0;
    }

    @Override
    public long skip(long n) throws IOException {
        return 0L;
    }
}
