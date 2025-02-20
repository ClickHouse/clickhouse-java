package com.clickhouse.data.stream;

import java.io.IOException;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

@Deprecated
public abstract class AbstractByteArrayOutputStream extends ClickHouseOutputStream {
    private static final Logger log = LoggerFactory.getLogger(AbstractByteArrayOutputStream.class);
    protected final byte[] buffer;

    protected int position;

    /**
     * Writes {@code buffer} by calling {@link #flushBuffer(byte[], int, int)} and
     * reset {@code position} to zero.
     *
     * @throws IOException when failed to flush buffer
     */
    protected void flushBuffer() throws IOException {
        flushBuffer(buffer, 0, position);
        position = 0;
    }

    /**
     * Writes the given byte array without using {@code buffer} and
     * {@code position}.
     *
     * @param bytes  non-null byte array to write
     * @param offset offset
     * @param length length in byte
     * @throws IOException when failed to write data
     */
    protected abstract void flushBuffer(byte[] bytes, int offset, int length) throws IOException;

    protected AbstractByteArrayOutputStream(ClickHousePassThruStream stream, int bufferSize, Runnable postCloseAction) {
        super(stream, postCloseAction);

        buffer = new byte[bufferSize];

        position = 0;
    }

    @Override
    public ClickHouseOutputStream transferBytes(byte[] bytes, int offset, int length) throws IOException {
        if (bytes == null) {
            throw new NullPointerException();
        } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        } else if (length == 0) {
            return this;
        }
        ensureOpen();

        if (position > 0) {
            flushBuffer();
        }
        flushBuffer(bytes, offset, length);
        return this;
    }

    @Override
    public ClickHouseOutputStream writeBuffer(ClickHouseByteBuffer buffer) throws IOException {
        if (buffer == null) {
            throw new NullPointerException();
        } else if (buffer.isEmpty()) {
            return this;
        }
        ensureOpen();

        byte[] b = this.buffer;
        int limit = b.length;
        int length = buffer.length();
        log.debug("writeBuffer limit:[{}] length:[{}] position:[{}]", limit, length, position);
        if (length <= limit - position) {
            System.arraycopy(buffer.array(), buffer.position(), b, position, length);
            position += length;
        } else {
            flushBuffer();
            flushBuffer(buffer.array(), buffer.position(), buffer.length());
        }
        return this;
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
