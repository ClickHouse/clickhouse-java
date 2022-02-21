package com.clickhouse.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.config.ClickHouseClientOption;

/**
 * Extended input stream for read optimization.
 */
public abstract class ClickHouseInputStream extends InputStream {
    /**
     * Empty byte array.
     */
    @Deprecated
    public static final byte[] EMPTY_BYTES = ClickHouseByteBuffer.EMPTY_BYTES;
    /**
     * Empty and read-only byte buffer.
     */
    @Deprecated
    public static final ByteBuffer EMPTY_BUFFER = ClickHouseByteBuffer.EMPTY_BUFFER;

    static final int MIN_BUFFER_SIZE = 1;
    static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;
    static final String INCOMPLETE_READ_ERROR = "Reached end of input stream after reading %d of %d bytes";

    static final class BlockingInputStream extends ClickHouseInputStream {
        private final BlockingQueue<ByteBuffer> queue;
        private final int timeout;

        // too much to maintain a 2-level buffer for reading?
        private ByteBuffer buffer;

        BlockingInputStream(BlockingQueue<ByteBuffer> queue, int timeout, Runnable afterClose) {
            super(afterClose);

            this.queue = ClickHouseChecker.nonNull(queue, "Queue");
            this.timeout = timeout > 0 ? timeout : 0;

            this.buffer = null;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException(
                        ClickHouseUtils.format("Blocking input stream(queue: %d, buffer: %d) has been closed",
                                queue.size(), buffer != null ? buffer.remaining() : 0));
            }

            if (buffer == null || (buffer != ClickHouseByteBuffer.EMPTY_BUFFER && !buffer.hasRemaining())) {
                updateBuffer();
            }
        }

        private int updateBuffer() throws IOException {
            try {
                if (timeout > 0) {
                    buffer = queue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (buffer == null) {
                        throw new IOException(ClickHouseUtils.format("Read timed out after %d ms", timeout));
                    }
                } else {
                    buffer = queue.take();
                }

                return buffer.remaining();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread was interrupted when getting next buffer from queue", e);
            }
        }

        @Override
        public int available() throws IOException {
            if (closed || buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
                return 0;
            }

            return (buffer == null || !buffer.hasRemaining()) ? updateBuffer() : buffer.remaining();
        }

        @Override
        public void close() throws IOException {
            // it's caller's responsiblity to consume all data in the queue, which will
            // unblock writer
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
                        byte[] bytes = buffer.array();
                        int pos = buffer.position();
                        output.write(bytes, pos, remain);
                        ((Buffer) buffer).position(pos + remain);
                    } else {
                        byte[] bytes = new byte[remain];
                        buffer.get(bytes);
                        output.write(bytes);
                    }
                    count += remain;
                }
                updateBuffer();
            }

            return count;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();

            if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
                return -1;
            }

            return 0xFF & buffer.get();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
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
                    len = 0;
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
        public ClickHouseByteBuffer read(int len) throws IOException {
            if (len <= 0) {
                return byteBuffer.reset();
            }

            ensureOpen();

            if (buffer == ClickHouseByteBuffer.EMPTY_BUFFER) {
                closeQuietly();
                throw new EOFException();
            } else if (buffer.remaining() >= len && buffer.hasArray()) {
                int position = buffer.position();
                byteBuffer.update(buffer.array(), position, len);
                ((Buffer) buffer).position(position + len);
            } else {
                byteBuffer.update(readBytes(len));
            }
            return byteBuffer;
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
                            : new IOException(ClickHouseUtils.format(INCOMPLETE_READ_ERROR, offset, length));
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
                while (buffer != ClickHouseByteBuffer.EMPTY_BUFFER && buffer.limit() > 0) {
                    counter += buffer.limit();
                    updateBuffer();
                }

                return counter;
            }

            return super.skip(n);
        }
    }

    static final class WrappedInputStream extends ClickHouseInputStream {
        private final InputStream in;
        private final byte[] buffer;

        private int position;
        private int limit;

        WrappedInputStream(InputStream input, int bufferSize, Runnable afterClose) {
            super(afterClose);

            this.in = ClickHouseChecker.nonNull(input, "InputStream");
            this.buffer = new byte[ClickHouseChecker.between(bufferSize, "BufferSize", MIN_BUFFER_SIZE,
                    MAX_BUFFER_SIZE)];

            position = 0;
            limit = 0;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException(ClickHouseUtils.format("Wrapped input stream(%s) has been closed", in));
            }
        }

        /**
         * Updates internal buffer backed by byte array.
         *
         * @return true if buffer has at least one byte available for read; false if
         *         input stream has been closed or reached end of stream
         * @throws IOException when failed to read data from input stream
         */
        private boolean updateBuffer() throws IOException {
            if (closed) {
                return false;
            }

            byte[] buf = buffer;
            int len = buf.length;
            int offset = 0;
            if (position > 0 && (offset = limit - position) > 0) {
                for (int i = 0; i < offset; i++) {
                    buf[i] = buf[position + i];
                }
            }

            while (offset < len) {
                int read = in.read(buf, offset, len - offset);
                if (read == -1) {
                    break;
                } else {
                    offset += read;
                }
            }

            limit = offset;
            position = 0;
            return limit > position;
        }

        @Override
        public int available() throws IOException {
            return limit > position || updateBuffer() ? limit - position : 0;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            try {
                in.close();
            } finally {
                position = 0;
                limit = 0;
                super.close();
            }
        }

        @Override
        public int peek() throws IOException {
            return limit > position || updateBuffer() ? 0xFF & buffer[position] : -1;
        }

        @Override
        public long pipe(ClickHouseOutputStream output) throws IOException {
            long count = 0L;
            if (output == null || output.isClosed()) {
                return count;
            }

            ensureOpen();

            int remain = limit - position;
            if (remain > 0) {
                output.write(buffer, position, remain);
                count += remain;
                position = limit;
            }

            while ((remain = in.read(buffer)) != -1) {
                output.write(buffer, 0, remain);
                count += remain;
            }

            return count;
        }

        @Override
        public int read() throws IOException {
            ensureOpen();

            int value = -1;
            if (position < limit || updateBuffer()) {
                value = 0xFF & buffer[position++];
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if ((len | off | b.length) < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (off == b.length) {
                throw new IOException("Nothing to read");
            } else if (buffer == b) {
                // in case b is the byte array return from ClickHouseByteBuffer.array()
                throw new IllegalArgumentException(
                        "Please pass a different byte array instead of internal buffer for reading");
            } else if (position + len <= limit) {
                System.arraycopy(buffer, position, b, off, len);
                position += len;
                return len;
            } else if (len <= buffer.length) {
                if (!updateBuffer()) {
                    return -1;
                }
                System.arraycopy(buffer, 0, b, off, limit);
                position = limit;
                return limit;
            }

            ensureOpen();

            int counter = 0;
            int remain = limit - position;
            if (remain > 0) {
                System.arraycopy(buffer, position, b, off, remain);
                counter += remain;
                off += remain;
            }

            while (counter < len) {
                int read = in.read(b, off, len - off);
                if (read == -1) {
                    break;
                } else {
                    off += read;
                }
            }

            return counter;
        }

        @Override
        public ClickHouseByteBuffer read(int len) throws IOException {
            if (len <= 0) {
                return byteBuffer.reset();
            }

            ensureOpen();

            if (position >= limit && !updateBuffer()) {
                closeQuietly();
                throw new EOFException();
            }

            int newLimit = position + len;
            if (limit >= newLimit) {
                byteBuffer.update(buffer, position, len);
                position = newLimit;
            } else {
                byteBuffer.update(readBytes(len));
            }
            return byteBuffer;
        }

        @Override
        public byte readByte() throws IOException {
            if (position < limit || updateBuffer()) {
                return buffer[position++];
            } else {
                closeQuietly();
                throw new EOFException();
            }
        }

        @Override
        public byte[] readBytes(int length) throws IOException {
            if (length < 1) {
                return ClickHouseByteBuffer.EMPTY_BYTES;
            } else if (position + length <= limit) {
                byte[] bytes = new byte[length];
                System.arraycopy(buffer, position, bytes, 0, length);
                position += length;
                return bytes;
            } else if (length <= buffer.length) {
                if (!updateBuffer()) {
                    closeQuietly();
                    throw new EOFException(
                            ClickHouseUtils.format("Failed to read %d bytes due to end of stream", length));
                } else if (length > limit) {
                    throw new EOFException(ClickHouseUtils.format("Reached end of stream after reading %d bytes of %d",
                            limit, length));
                }
                byte[] bytes = new byte[length];
                System.arraycopy(buffer, position, bytes, 0, length);
                position += length;
                return bytes;
            }

            ensureOpen();

            byte[] bytes = new byte[length];
            int counter = 0;
            while (counter < length) {
                if (limit > position || updateBuffer()) {
                    int size = Math.min(limit - position, length - counter);
                    System.arraycopy(buffer, position, bytes, counter, size);
                    position += size;
                    counter += size;
                } else {
                    closeQuietly();
                    throw counter == 0 ? new EOFException()
                            : new IOException(ClickHouseUtils.format(INCOMPLETE_READ_ERROR, counter, bytes.length));
                }
            }
            return bytes;
        }

        @Override
        public long skip(long n) throws IOException {
            ensureOpen();

            long counter = 0L;
            while (n > 0L) {
                if (limit > position || updateBuffer()) {
                    int remain = limit - position;
                    if (n > remain) {
                        n -= remain;
                        counter += remain;
                        position = limit;
                    } else {
                        counter += n;
                        position += n;
                        n = 0L;
                    }
                } else {
                    break;
                }
            }

            return counter;
        }
    }

    /**
     * Wraps the given blocking queue.
     *
     * @param queue   non-null blocking queue
     * @param timeout read timeout in milliseconds
     * @return wrapped input
     */
    public static ClickHouseInputStream of(BlockingQueue<ByteBuffer> queue, int timeout) {
        return new BlockingInputStream(queue, timeout, null);
    }

    /**
     * Wraps the given blocking queue.
     *
     * @param queue      non-null blocking queue
     * @param timeout    read timeout in milliseconds
     * @param afterClose custom handler will be invoked right after closing the
     *                   input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(BlockingQueue<ByteBuffer> queue, int timeout, Runnable afterClose) {
        return new BlockingInputStream(queue, timeout, afterClose);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input non-null input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input) {
        return of(input, (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue(), null);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input      non-null input stream
     * @param bufferSize buffer size which is always greater than zero(usually 4096
     *                   or larger)
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize) {
        return of(input, bufferSize, null);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input      non-null input stream
     * @param bufferSize buffer size which is always greater than zero(usually 4096
     *                   or larger)
     * @param afterClose custom handler will be invoked right after closing the
     *                   input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize, Runnable afterClose) {
        return input instanceof ClickHouseInputStream ? (ClickHouseInputStream) input
                : new WrappedInputStream(input, bufferSize, afterClose);
    }

    protected final Runnable afterClose;
    protected final ClickHouseByteBuffer byteBuffer;

    protected boolean closed;

    protected ClickHouseInputStream(Runnable afterClose) {
        this.afterClose = afterClose;
        this.byteBuffer = ClickHouseByteBuffer.newInstance();

        this.closed = false;
    }

    protected void closeQuietly() {
        try {
            close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Peeks one byte. It's similar as {@link #read()} except it never changes
     * cursor.
     *
     * @return the next byte of data, or -1 if the end of the stream is reached
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public abstract int peek() throws IOException;

    /**
     * Reads all bytes and write into given output stream.
     *
     * @param output non-null output stream
     * @return bytes being written into output stream
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public abstract long pipe(ClickHouseOutputStream output) throws IOException;

    /**
     * Reads an unsigned byte from the input stream. Unlike {@link #read()}, it will
     * throw {@link IOException} if the input stream has been closed.
     *
     * @return unsigned byte
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public int readUnsignedByte() throws IOException {
        return 0xFF & readByte();
    }

    /**
     * Reads one single byte from the input stream. Unlike {@link #read()}, it will
     * throw {@link IOException} if the input stream has been closed. In general,
     * this method should be faster than {@link #read()}, especially when it's an
     * input stream backed by byte[] or {@link java.nio.ByteBuffer}.
     *
     * @return byte value if present
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public abstract byte readByte() throws IOException;

    /**
     * Reads {@code length} bytes from the input stream. It behaves in the same
     * way as {@link java.io.DataInput#readFully(byte[])}, except it will throw
     * {@link IOException} when the input stream has been closed.
     *
     * @param length number of bytes to read
     * @return byte array and its length should be {@code length}
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public byte[] readBytes(int length) throws IOException {
        if (length <= 0) {
            return ClickHouseByteBuffer.EMPTY_BYTES;
        } else if (closed) {
            throw new IOException("Stream has been closed");
        }

        byte[] bytes = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = read(bytes, offset, length - offset);
            if (read == -1) {
                closeQuietly();
                throw offset == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils.format(INCOMPLETE_READ_ERROR, offset, length));
            } else {
                offset += read;
            }
        }

        return bytes;
    }

    public ClickHouseByteBuffer read(int len) throws IOException {
        return len <= 0 ? byteBuffer.reset() : byteBuffer.update(readBytes(len));
    }

    /**
     * Reads string from the input stream. {@link #readVarInt()} will be called
     * automatically to understand byte length of the string.
     *
     * @param charset charset, null is treated as {@link StandardCharsets#UTF_8}
     * @return non-null string
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public String readString(Charset charset) throws IOException {
        return readString(readVarInt(), charset);
    }

    /**
     * Reads string from the input stream. When {@code byteLength} is zero or
     * negative number, this method will always return empty string.
     *
     * @param byteLength length in byte
     * @param charset    charset, null is treated as {@link StandardCharsets#UTF_8}
     * @return non-null string
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public String readString(int byteLength, Charset charset) throws IOException {
        if (byteLength < 1) {
            return "";
        }

        ClickHouseByteBuffer buf = read(byteLength);
        return new String(buf.array, buf.position, buf.length, charset != null ? charset : StandardCharsets.UTF_8);
    }

    /**
     * Reads ascii string from input stream. {@link #readVarInt()} will be called
     * automatically to understand byte length of the string.
     *
     * @return non-null ascii string
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public String readAsciiString() throws IOException {
        return readString(readVarInt(), StandardCharsets.US_ASCII);
    }

    /**
     * Reads ascii string from input stream. Similar as
     * {@code readString(byteLength, StandardCharsets.US_ASCII)}.
     *
     * @param byteLength length in byte
     * @return non-null ascii string
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public String readAsciiString(int byteLength) throws IOException {
        return readString(byteLength, StandardCharsets.US_ASCII);
    }

    /**
     * Reads unicode string from input stream.
     *
     * @return non-null unicode string
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public String readUnicodeString() throws IOException {
        return readString(readVarInt(), StandardCharsets.UTF_8);
    }

    /**
     * Reads unicode string from input stream. Similar as
     * {@code readString(byteLength, null)}.
     *
     * @param byteLength length in byte
     * @return non-null unicode string
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public String readUnicodeString(int byteLength) throws IOException {
        return readString(byteLength, StandardCharsets.UTF_8);
    }

    /**
     * Read varint from input stream.
     *
     * @return varint
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public int readVarInt() throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/abe314feecd1647d7c2b952a25da7abf5c19f352/src/IO/VarInt.h#L126
        long result = 0L;
        int shift = 0;
        for (int i = 0; i < 9; i++) {
            // gets 7 bits from next byte
            byte b = readByte();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
        }

        return (int) result;
    }

    /**
     * Checks if the input stream has been closed or not.
     *
     * @return true if the input stream has been closed; false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            // don't want to hold the last byte array reference for too long
            byteBuffer.reset();
            if (afterClose != null) {
                afterClose.run();
            }
        }
    }
}
