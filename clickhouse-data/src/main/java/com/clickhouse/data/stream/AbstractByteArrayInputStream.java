package com.clickhouse.data.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.util.Arrays;
import java.util.LinkedList;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseByteUtils;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseUtils;

/**
 * Byte-array backed input stream.
 */
@Deprecated
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

    @Override
    protected ClickHouseByteBuffer getBuffer() {
        return position < limit ? byteBuffer.update(buffer, position, limit - position) : byteBuffer.reset();
    }

    @Override
    protected ClickHouseByteBuffer nextBuffer() throws IOException {
        if (closed) {
            return byteBuffer.reset();
        }

        if (limit > position || updateBuffer() > 0) {
            byteBuffer.update(buffer, position, limit - position);
            position = limit;
        } else {
            byteBuffer.setLength(0);
        }
        return byteBuffer;
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
        // buffer = ClickHouseByteBuffer.EMPTY_BYTES;
        super.close();
    }

    @Override
    public int peek() throws IOException {
        ensureOpen();

        return position < limit ? 0xFF & buffer[position] : -1;
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
    public ClickHouseByteBuffer readBufferUntil(byte[] separator) throws IOException {
        if (separator == null || separator.length < 1) {
            return byteBuffer.reset();
        }
        ensureOpen();

        boolean copyBuffer = reusableBuffer();
        int slen = separator.length;
        LinkedList<byte[]> list = new LinkedList<>();
        int offset = copyBuffer ? 0 : position;
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = limit - position;
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int read = ClickHouseByteUtils.indexOf(buffer, position, remain, separator, 0, slen, true);
                int missed = 0;
                if (read == -1 || (missed = slen + read - limit) > 0) {
                    while (true) {
                        list.add(copyBuffer ? Arrays.copyOfRange(buffer, position, limit) : buffer);
                        length += remain;
                        position = limit;
                        if ((remain = updateBuffer()) < 1) {
                            closeQuietly();
                            more = false;
                            break;
                        }

                        if (missed > 0) {
                            if (remain < missed) {
                                if (ClickHouseByteUtils.equals(buffer, position, missed, separator, slen - missed,
                                        slen - missed + missed)) {
                                    missed -= remain;
                                } else {
                                    missed = 0;
                                }
                            } else {
                                if (ClickHouseByteUtils.equals(buffer, position, missed, separator, slen - missed,
                                        slen)) {
                                    if (!copyBuffer && remain == missed) {
                                        list.add(buffer);
                                    } else {
                                        byte[] bytes = new byte[missed];
                                        System.arraycopy(buffer, position, bytes, 0, missed);
                                        list.add(bytes);
                                    }
                                    length += missed;
                                    position += missed;
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
                    if (copyBuffer) {
                        list.add(Arrays.copyOfRange(buffer, position, read));
                    } else {
                        list.add(buffer);
                    }
                    length += read - position;
                    position = read;
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, offset, length);
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        if (reader == null) {
            return byteBuffer.reset();
        }
        ensureOpen();

        boolean copyBuffer = reusableBuffer();
        LinkedList<byte[]> list = new LinkedList<>();
        int offset = copyBuffer ? 0 : position;
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
                    list.add(copyBuffer ? Arrays.copyOfRange(buffer, position, limit) : buffer);
                    length += remain;
                    position = limit;
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    if (read > 0) {
                        if (copyBuffer) {
                            byte[] bytes = new byte[read];
                            System.arraycopy(buffer, position, bytes, 0, read);
                            list.add(bytes);
                        } else {
                            list.add(buffer);
                        }
                        length += read;
                        position += read;
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
                        : new StreamCorruptedException(
                                ClickHouseUtils.format(ERROR_INCOMPLETE_READ, count, bytes.length));
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
