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
    public static final byte[] EMPTY_BYTES = new byte[0];
    /**
     * Empty and read-only byte buffer.
     */
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(EMPTY_BYTES).asReadOnlyBuffer();

    static final class BlockingInputStream extends ClickHouseInputStream {
        private final BlockingQueue<ByteBuffer> queue;
        private final int timeout;

        // too much to maintain a 2-level buffer for reading?
        private ByteBuffer buffer;
        private boolean closed;

        BlockingInputStream(BlockingQueue<ByteBuffer> queue, int timeout) {
            this.queue = ClickHouseChecker.nonNull(queue, "Queue");
            this.timeout = timeout;

            this.buffer = null;
            this.closed = false;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException(
                        ClickHouseUtils.format("Blocking stream(queue: %d, buffer: %d) has been closed",
                                queue.size(), buffer != null ? buffer.remaining() : 0));
            }

            if (buffer == null || (buffer != EMPTY_BUFFER && !buffer.hasRemaining())) {
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
            if (closed || buffer == EMPTY_BUFFER) {
                return 0;
            }

            return (buffer == null || !buffer.hasRemaining()) ? updateBuffer() : buffer.remaining();
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            // it's caller's responsiblity to consume all data in the queue, which will
            // unblock writer
            closed = true;
            buffer = null;
        }

        @Override
        public byte readByte() throws IOException {
            ensureOpen();

            if (buffer == EMPTY_BUFFER) {
                close();
                throw new EOFException();
            }

            return buffer.get();
        }

        @Override
        public int read() throws IOException {
            ensureOpen();

            if (buffer == EMPTY_BUFFER) {
                return -1;
            }

            return 0xFF & buffer.get();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ensureOpen();

            int counter = 0;
            while (len > 0) {
                if (buffer == EMPTY_BUFFER) {
                    return counter > 0 ? counter : -1;
                }

                int remain = buffer.remaining();
                if (remain >= len) {
                    buffer.get(b, off, len);
                    counter += len;
                    len = 0;
                } else {
                    buffer.get(b, off, remain);
                    counter += remain;
                    off += remain;
                    len -= remain;

                    updateBuffer();
                }
            }

            return counter;
        }

        @Override
        public String readString(int byteLength, Charset charset) throws IOException {
            ensureOpen();

            if (byteLength < 1) {
                return "";
            }

            if (charset == null) {
                charset = StandardCharsets.UTF_8;
            }

            if (!buffer.isReadOnly() && byteLength > 8 && buffer.remaining() > byteLength) {
                int pos = buffer.position();
                ((Buffer) buffer).position(pos + byteLength);
                return charset.decode(ByteBuffer.wrap(buffer.array(), pos, byteLength)).toString();
            }

            return new String(readBytes(byteLength), charset);
        }

        @Override
        public long skip(long n) throws IOException {
            ensureOpen();

            // peforms better but this is a bit tricky
            if (n == Long.MAX_VALUE) {
                long counter = buffer.remaining();
                while (buffer != EMPTY_BUFFER && buffer.limit() > 0) {
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
        private boolean closed;

        WrappedInputStream(InputStream input, int bufferSize) {
            in = ClickHouseChecker.nonNull(input, "InputStream");
            buffer = new byte[bufferSize];
            position = 0;
            limit = 0;
            closed = false;
        }

        private void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException(ClickHouseUtils.format("Wrapped stream(%s) has been closed", in));
            }
        }

        private int updateBuffer() throws IOException {
            if (closed) {
                return -1;
            }

            position = 0;
            int count = in.read(buffer);
            limit = count > 0 ? count : 0;
            return count;
        }

        @Override
        public int available() throws IOException {
            return !closed && (position < limit || updateBuffer() > 0) ? limit - position : 0;
        }

        @Override
        public byte readByte() throws IOException {
            if (position >= limit && updateBuffer() < 0) {
                try {
                    close();
                } catch (IOException e) {
                    // ignore
                }
                throw new EOFException();
            }

            return buffer[position++];
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                try {
                    in.close();
                } finally {
                    closed = true;
                    position = 0;
                    limit = 0;
                }
            }
        }

        @Override
        public int read() throws IOException {
            ensureOpen();

            int value = -1;
            if (position < limit || updateBuffer() > 0) {
                value = 0xFF & buffer[position++];
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (position >= limit && updateBuffer() < 0) {
                return -1;
            }

            ensureOpen();

            int counter = 0;
            while (counter < len) {
                int size = Math.min(limit - position, len - counter);
                System.arraycopy(buffer, position, b, off, size);
                position += size;
                off += size;
                counter += size;

                if (position >= limit && updateBuffer() < 0) {
                    break;
                }
            }

            return counter;
        }

        @Override
        public byte[] readBytes(int length) throws IOException {
            if (length <= 0) {
                return EMPTY_BYTES;
            }

            ensureOpen();

            byte[] bytes = new byte[length];
            int offset = 0;
            int counter = 0;
            while (counter < length) {
                if (position >= limit && updateBuffer() < 0) {
                    try {
                        close();
                    } catch (IOException e) {
                        // ignore
                    }
                    throw counter == 0 ? new EOFException()
                            : new IOException(ClickHouseUtils
                                    .format("Reached end of input stream after reading %d of %d bytes", counter,
                                            bytes.length));
                }

                int size = Math.min(limit - position, length - counter);
                System.arraycopy(buffer, position, bytes, offset, size);
                position += size;
                offset += size;
                counter += size;
            }

            return bytes;
        }

        @Override
        public String readString(int byteLength, Charset charset) throws IOException {
            ensureOpen();

            if (byteLength < 1) {
                return "";
            }

            if (charset == null) {
                charset = StandardCharsets.UTF_8;
            }

            if (limit - position > byteLength) {
                int offset = position;
                position += byteLength;
                return new String(buffer, offset, byteLength, charset);
            }

            return new String(readBytes(byteLength), charset);
        }

        @Override
        public long skip(long n) throws IOException {
            ensureOpen();

            long counter = 0L;
            while (n > 0L) {
                if (position >= limit && updateBuffer() < 0) {
                    break;
                } else {
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
        return new BlockingInputStream(queue, timeout);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input non-null input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input) {
        return of(input, (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue());
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
        return input instanceof ClickHouseInputStream ? (ClickHouseInputStream) input
                : new WrappedInputStream(input, bufferSize);
    }

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
     * way as {@link java.io.DataInput#readFully(byte[])}, and it will throw
     * {@link IOException} when the input stream has been closed.
     *
     * @param length number of bytes to read
     * @return byte array and its length should be {@code length}
     * @throws IOException when failed to read value from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public byte[] readBytes(int length) throws IOException {
        if (length <= 0) {
            return EMPTY_BYTES;
        }

        byte[] bytes = new byte[length];

        for (int l = length, c = 0, n = 0; l > 0; l -= n) {
            n = read(bytes, c, l);
            if (n != -1) {
                c += n;
            } else {
                try {
                    close();
                } catch (IOException e) {
                    // ignore
                }

                throw c == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils
                                .format("Reached end of input stream after reading %d of %d bytes", c, length));
            }
        }

        return bytes;
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

        return new String(readBytes(byteLength), charset != null ? charset : StandardCharsets.UTF_8);
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
    public abstract boolean isClosed();
}
