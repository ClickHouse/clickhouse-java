package com.clickhouse.client.stream;

import java.io.IOException;

import com.clickhouse.client.ClickHouseDataUpdater;
import com.clickhouse.client.ClickHouseOutputStream;

public abstract class AbstractByteArrayOutputStream extends ClickHouseOutputStream {
    protected final byte[] buffer;

    protected int position;

    protected abstract void flushBuffer() throws IOException;

    protected AbstractByteArrayOutputStream(int bufferSize, Runnable postCloseAction) {
        super(postCloseAction);

        buffer = new byte[bufferSize];

        position = 0;
    }

    @Override
    public ClickHouseOutputStream writeByte(byte b) throws IOException {
        ensureOpen();

        buffer[position++] = b;
        if (position >= buffer.length) {
            flushBuffer();
        }
        return this;
    }

    @Override
    public ClickHouseOutputStream writeBytes(byte[] bytes, int offset, int length) throws IOException {
        if (bytes == null) {
            throw new NullPointerException();
        } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        } else if (length == 0) {
            return this;
        }
        ensureOpen();

        int limit = buffer.length;
        while (length > 0) {
            int remain = limit - position;
            if (length < remain) {
                System.arraycopy(bytes, offset, buffer, position, length);
                position += length;
                length = 0;
            } else {
                System.arraycopy(bytes, offset, buffer, position, remain);
                position = limit;
                offset += remain;
                length -= remain;
                flushBuffer();
            }
        }
        return this;
    }

    @Override
    public ClickHouseOutputStream writeCustom(ClickHouseDataUpdater writer) throws IOException {
        ensureOpen();

        int written = 0;
        while ((written = writer.update(buffer, position, buffer.length)) < 0) {
            flushBuffer();
        }
        position += written;
        return this;
    }
}
