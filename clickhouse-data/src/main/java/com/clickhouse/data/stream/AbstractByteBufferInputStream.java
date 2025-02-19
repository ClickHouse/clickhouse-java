package com.clickhouse.data.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseUtils;

/**
 * {@link java.nio.ByteBuffer} backed input stream.
 */
@Deprecated
public abstract class AbstractByteBufferInputStream extends ClickHouseInputStream {
    protected ByteBuffer buffer;

    protected AbstractByteBufferInputStream(ClickHouseFile file, OutputStream copyTo, Runnable postCloseAction) {
        super(file, copyTo, postCloseAction);

        this.buffer = null;
    }

    @Override
    protected void ensureOpen() throws IOException {
        super.ensureOpen();

        if (buffer == null || (buffer != ClickHouseByteBuffer.EMPTY_BUFFER && !buffer.hasRemaining())) {
            updateBuffer();
        }
    }

    @Override
    protected ClickHouseByteBuffer getBuffer() {
        return byteBuffer.update(buffer);
    }

    @Override
    protected ClickHouseByteBuffer nextBuffer() throws IOException {
        if (closed) {
            return byteBuffer.reset();
        }

        if ((buffer != null && buffer.hasRemaining()) || updateBuffer() > 0) {
            byteBuffer.update(buffer);
            ((Buffer) buffer).position(buffer.limit());
        } else {
            byteBuffer.setLength(0);
        }
        return byteBuffer;
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
        if (closed) {
            return;
        }
        // buffer = null;
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
    public ClickHouseByteBuffer readBufferUntil(byte[] separator) throws IOException {
        if (separator == null || separator.length < 1) {
            return byteBuffer.reset();
        }
        ensureOpen();

        int slen = separator.length;
        LinkedList<byte[]> list = new LinkedList<>();
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = buffer.remaining();
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int position = buffer.position();
                int limit = buffer.limit();
                byte[] bytes = ClickHouseByteUtils.getOrCopy(buffer, remain);
                int read = ClickHouseByteUtils.indexOf(bytes, 0, remain, separator, 0, slen, true);
                int missed = 0;
                if (read == -1 || (missed = slen + read - limit) > 0) {
                    while (true) {
                        list.add(bytes);
                        length += remain;
                        ((Buffer) buffer).position(limit);
                        if ((remain = updateBuffer()) < 1) {
                            closeQuietly();
                            more = false;
                            break;
                        }

                        if (missed > 0) {
                            bytes = ClickHouseByteUtils.getOrCopy(buffer, remain);
                            if (remain < missed) {
                                if (ClickHouseByteUtils.equals(bytes, 0, missed, separator, slen - missed,
                                        slen - missed + missed)) {
                                    missed -= remain;
                                } else {
                                    missed = 0;
                                }
                            } else {
                                if (ClickHouseByteUtils.equals(bytes, 0, missed, separator, slen - missed,
                                        slen)) {
                                    length += missed;
                                    ((Buffer) buffer).position(missed);
                                    list.add(Arrays.copyOfRange(bytes, 0, missed));
                                    more = false;
                                }
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    read += slen;
                    list.add(bytes);
                    length += read;
                    ((Buffer) buffer).position(position + read);
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, 0, length);
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        if (reader == null) {
            return byteBuffer.reset();
        }
        ensureOpen();

        LinkedList<byte[]> list = new LinkedList<>();
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = buffer.remaining();
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int position = buffer.position();
                int limit = buffer.limit();
                byte[] bytes = ClickHouseByteUtils.getOrCopy(buffer, remain);
                int read = reader.update(bytes, 0, remain);
                if (read == -1) {
                    list.add(bytes);
                    length += remain;
                    ((Buffer) buffer).position(limit);
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    if (read > 0) {
                        list.add(Arrays.copyOfRange(bytes, 0, read));
                        length += read;
                        ((Buffer) buffer).position(position + read);
                    }
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, 0, length);
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
        ByteBuffer b = buffer;
        int remain = b.remaining();
        while (length > 0) {
            if (b == ClickHouseByteBuffer.EMPTY_BUFFER) {
                closeQuietly();
                throw offset == 0 ? new EOFException()
                        : new StreamCorruptedException(
                                ClickHouseUtils.format(ERROR_INCOMPLETE_READ, offset, bytes.length));
            }

            if (remain >= length) {
                b.get(bytes, offset, length);
                offset += length;
                length = 0;
            } else {
                b.get(bytes, offset, remain);
                offset += remain;
                length -= remain;
                remain = updateBuffer();
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
            long counter = buffer.remaining();
            while (buffer != ClickHouseByteBuffer.EMPTY_BUFFER) {
                counter += updateBuffer();
            }

            return counter;
        }

        return super.skip(n);
    }
}
