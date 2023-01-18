package com.clickhouse.data.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseUtils;

/**
 * Byte-array backed input stream.
 */
public abstract class AbstractByteArrayInputStream extends ClickHouseInputStream {
    protected byte[] buffer;
    protected int position;
    protected int limit;

    protected AbstractByteArrayInputStream(ClickHousePassThruStream stream, OutputStream copyTo,
            Runnable postCloseAction) {
        super(stream, copyTo, postCloseAction);

        buffer = ClickHouseByteBuffer.EMPTY_BYTES;
        position = 0;
        limit = buffer.length;
    }

    @Override
    protected void ensureOpen() throws IOException {
        super.ensureOpen();

        if (position >= limit) {
            updateBuffer();
        }
    }

    /**
     * Update inner byte array along with {@code position} and {@code limit} as
     * needed.
     *
     * @return remaining bytes in buffer
     * @throws IOException when failed to read value
     */
    protected abstract int updateBuffer() throws IOException;

    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }

        int remain = limit - position;
        return remain > 0 ? remain : updateBuffer();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        buffer = ClickHouseByteBuffer.EMPTY_BYTES;
        super.close();
    }

    @Override
    public int peek() throws IOException {
        ensureOpen();

        return position < limit ? 0xFF & buffer[position] : -1;
    }

    @Override
    public long pipe(ClickHouseOutputStream output) throws IOException {
        long count = 0L;
        if (output == null || output.isClosed()) {
            return count;
        }
        ensureOpen();

        try {
            byte[] b = buffer;
            int l = limit;
            int p = position;
            int remain = l - p;
            if (remain > 0) {
                output.transferBytes(b, p, remain);
                count += remain;
                while ((remain = updateBuffer()) > 0) {
                    b = buffer;
                    output.transferBytes(b, 0, remain);
                    count += remain;
                }
            }
        } finally {
            close();
        }
        return count;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();

        return position < limit ? 0xFF & buffer[position++] : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException(ERROR_NULL_BYTES);
        } else if (buffer == b) {
            // in case b is the byte array return from ClickHouseByteBuffer.array()
            throw new IllegalArgumentException(ERROR_REUSE_BUFFER);
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        ensureOpen();

        int remain = limit - position;
        if (remain < 1) {
            return -1;
        } else if (remain >= len) {
            System.arraycopy(buffer, position, b, off, len);
            position += len;
            return len;
        } else {
            System.arraycopy(buffer, position, b, off, remain);
            off += remain;
            len -= remain;
        }

        int count = remain; // should be greater than zero
        while (len > 0 && (remain = updateBuffer()) > 0) {
            if (remain >= len) {
                System.arraycopy(buffer, position, b, off, len);
                position += len;
                return count + len;
            } else {
                System.arraycopy(buffer, position, b, off, remain);
                off += remain;
                len -= remain;
                count += remain;
            }
        }

        return count;
    }

    @Override
    public ClickHouseByteBuffer readBuffer(int length) throws IOException {
        if (length < 1) {
            return byteBuffer.reset();
        }
        ensureOpen();

        if (position >= limit) {
            closeQuietly();
            throw new EOFException();
        }

        int newLimit = position + length;
        if (limit >= newLimit) {
            byteBuffer.update(buffer, position, length);
            position = newLimit;
        } else {
            byteBuffer.update(readBytes(length));
        }
        return byteBuffer;
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        if (reader == null) {
            return byteBuffer.reset();
        }
        ensureOpen();

        LinkedList<byte[]> list = new LinkedList<>();
        int offset = position;
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = limit - position;
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int read = reader.update(buffer, position, limit);
                if (read == -1) {
                    length += remain;
                    position = limit;
                    list.add(buffer);
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    if (read > 0) {
                        length += read;
                        position += read;
                        list.add(buffer);
                    }
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, offset, length);
    }

    @Override
    public byte readByte() throws IOException {
        ensureOpen();

        if (position >= limit) {
            closeQuietly();
            throw new EOFException();
        }

        return buffer[position++];
    }

    @Override
    public byte[] readBytes(int length) throws IOException {
        if (length < 1) {
            return ClickHouseByteBuffer.EMPTY_BYTES;
        }
        ensureOpen();

        byte[] bytes = new byte[length];
        byte[] b = buffer;
        int count = 0;
        int l = limit;
        int p = position;
        int remain = l - p;
        while (length > 0) {
            if (remain < 1) {
                closeQuietly();
                throw count == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, count, bytes.length));
            }

            if (remain >= length) {
                System.arraycopy(b, p, bytes, count, length);
                position = p + length;
                count += length;
                length = 0;
            } else {
                System.arraycopy(b, p, bytes, count, remain);
                count += remain;
                length -= remain;
                remain = updateBuffer();
                p = position;
                b = buffer;
            }
        }
        return bytes;
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();

        // peforms better but this is a bit tricky
        if (n == Long.MAX_VALUE) {
            int avail = 0;
            long counter = (long) limit - position;
            while ((avail = updateBuffer()) > 0) {
                counter += avail;
            }

            return counter;
        }

        return super.skip(n);
    }
}
