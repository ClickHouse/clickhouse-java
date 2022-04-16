package com.clickhouse.client.stream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseDataUpdater;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseUtils;

/**
 * {@link java.nio.ByteBuffer} backed input stream.
 */
public abstract class AbstractByteBufferInputStream extends ClickHouseInputStream {
    protected ByteBuffer buffer;

    protected AbstractByteBufferInputStream(Runnable postCloseAction) {
        super(postCloseAction);

        this.buffer = null;
    }

    @Override
    protected void ensureOpen() throws IOException {
        super.ensureOpen();

        if (buffer == null || (buffer != ClickHouseByteBuffer.EMPTY_BUFFER && !buffer.hasRemaining())) {
            updateBuffer();
        }
    }

    /**
     * Update inner buffer as needed.
     *
     * @return remaining bytes in buffer
     * @throws IOException when failed to update buffer
     */
    protected abstract int updateBuffer() throws IOException;

    @Override
    public int available() throws IOException {
        if (closed || buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
            return 0;
        }

        return buffer != null && buffer.hasRemaining() ? buffer.remaining() : updateBuffer();
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        super.close();
    }

    @Override
    public int peek() throws IOException {
        ensureOpen();

        if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
            return -1;
        }

        int b = 0xFF & buffer.get();
        ((Buffer) buffer).position(buffer.position() - 1);
        return b;
    }

    @Override
    public long pipe(ClickHouseOutputStream output) throws IOException {
        long count = 0L;
        if (output == null || output.isClosed()) {
            return count;
        }
        ensureOpen();

        while (buffer != ClickHouseByteBuffer.EMPTY_BUFFER) {
            int remain = buffer.remaining();
            if (remain > 0) {
                if (buffer.hasArray()) {
                    output.write(buffer.array(), buffer.position(), remain);
                } else {
                    byte[] bytes = new byte[remain];
                    buffer.get(bytes);
                    output.write(bytes);
                }
                count += remain;
            }
            updateBuffer();
        }
        close();
        return count;
    }

    @Override
    public int read() throws IOException {
        ensureOpen();

        return buffer == ClickHouseByteBuffer.EMPTY_BUFFER ? -1 : 0xFF & buffer.get();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException(ERROR_NULL_BYTES);
        } else if (buffer != null && buffer.hasArray() && buffer.array() == b) {
            // in case b is the byte array return from ClickHouseByteBuffer.array()
            throw new IllegalArgumentException(ERROR_REUSE_BUFFER);
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        ensureOpen();

        int offset = off;
        while (len > 0) {
            if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
                return off > offset ? off - offset : -1;
            }

            int remain = buffer.remaining();
            if (remain >= len) {
                buffer.get(b, off, len);
                off += len;
                break;
            } else {
                buffer.get(b, off, remain);
                off += remain;
                len -= remain;
                updateBuffer();
            }
        }

        return off - offset;
    }

    @Override
    public ClickHouseByteBuffer readBuffer(int length) throws IOException {
        if (length < 1) {
            return byteBuffer.reset();
        }
        ensureOpen();

        if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
            closeQuietly();
            throw new EOFException();
        } else if (buffer.remaining() >= length && buffer.hasArray()) {
            int position = buffer.position();
            byteBuffer.update(buffer.array(), position, length);
            ((Buffer) buffer).position(position + length);
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
        int offset = 0;
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = buffer.remaining();
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int position = buffer.position();
                int startPos = position;
                int limit = buffer.limit();
                int endPos = limit;
                byte[] bytes;
                if (buffer.hasArray() && buffer.capacity() == buffer.limit()) {
                    bytes = buffer.array();
                    if (bytes.length > remain) {
                        byte[] newBytes = new byte[remain];
                        System.arraycopy(bytes, position, newBytes, 0, remain);
                        bytes = newBytes;
                        startPos = 0;
                        endPos = remain;
                    }
                } else {
                    bytes = new byte[remain];
                    buffer.get(bytes);
                    ((Buffer) buffer).position(position);
                    startPos = 0;
                    endPos = remain;
                }
                int read = reader.update(bytes, startPos, endPos);
                if (read == -1) {
                    length += remain;
                    ((Buffer) buffer).position(limit);
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    if (read > 0) {
                        length += read;
                        ((Buffer) buffer).position(position + read);
                    }
                    more = false;
                }
                list.add(bytes);
            }
        }
        return byteBuffer.update(list, offset, length);
    }

    @Override
    public byte readByte() throws IOException {
        ensureOpen();

        if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
            closeQuietly();
            throw new EOFException();
        }

        return buffer.get();
    }

    @Override
    public byte[] readBytes(int length) throws IOException {
        if (length < 1) {
            return ClickHouseByteBuffer.EMPTY_BYTES;
        }
        ensureOpen();

        byte[] bytes = new byte[length];
        int offset = 0;
        int len = length;
        while (len > 0) {
            if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
                closeQuietly();
                throw offset == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, offset, length));
            }

            int remain = buffer.remaining();
            if (remain >= len) {
                buffer.get(bytes, offset, len);
                offset += len;
                len = 0;
            } else {
                buffer.get(bytes, offset, remain);
                offset += remain;
                len -= remain;
                updateBuffer();
            }
        }

        return bytes;
    }

    @Override
    public long skip(long n) throws IOException {
        ensureOpen();

        // peforms better but this is a bit tricky
        if (n == Long.MAX_VALUE) {
            long counter = buffer.remaining();
            while (buffer != ClickHouseByteBuffer.EMPTY_BUFFER) {
                counter += buffer.limit();
                updateBuffer();
            }

            return counter;
        }

        return super.skip(n);
    }
}
