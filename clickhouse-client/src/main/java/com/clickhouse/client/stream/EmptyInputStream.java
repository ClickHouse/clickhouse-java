package com.clickhouse.client.stream;

import java.io.EOFException;
import java.io.IOException;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseDataUpdater;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;

/**
 * Empty input stream produces nothing and it can never be closed.
 */
public final class EmptyInputStream extends ClickHouseInputStream {
    public static final EmptyInputStream INSTANCE = new EmptyInputStream();

    private EmptyInputStream() {
        super(null, null, null);
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
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        return byteBuffer.reset();
    }

    @Override
    public int read() throws IOException {
        return -1;
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
