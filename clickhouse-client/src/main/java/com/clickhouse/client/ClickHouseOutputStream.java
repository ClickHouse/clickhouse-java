package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.clickhouse.client.config.ClickHouseClientOption;

public abstract class ClickHouseOutputStream extends OutputStream {
    static class WrappedOutputStream extends ClickHouseOutputStream {
        private final byte[] buffer;
        private final OutputStream out;

        private int count;

        private void flushBuffer() throws IOException {
            if (count > 0) {
                out.write(buffer, 0, count);
                count = 0;
            }
        }

        protected WrappedOutputStream(OutputStream out, int bufferSize, Runnable afterClose) {
            super(afterClose);

            this.buffer = new byte[bufferSize <= 0 ? 8192 : bufferSize];
            this.out = ClickHouseChecker.nonNull(out, "OutputStream");

            this.count = 0;
        }

        protected void ensureOpen() throws IOException {
            if (closed) {
                throw new IOException("Cannot operate on a closed output stream");
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            try {
                flushBuffer();
                out.close();
            } finally {
                super.close();
            }
        }

        @Override
        public void flush() throws IOException {
            ensureOpen();

            flushBuffer();
            out.flush();
        }

        @Override
        public ClickHouseOutputStream writeByte(byte b) throws IOException {
            ensureOpen();

            if (count >= buffer.length) {
                flushBuffer();
            }
            buffer[count++] = b;
            return this;
        }

        @Override
        public ClickHouseOutputStream writeBytes(byte[] bytes, int offset, int length) throws IOException {
            ensureOpen();

            int len = buffer.length;
            if (length >= len) {
                flushBuffer();
                out.write(bytes, offset, length);
            } else {
                if (length > len - count) {
                    flushBuffer();
                }
                System.arraycopy(bytes, offset, buffer, count, length);
                count += length;
            }
            return this;
        }
    }

    /**
     * Wraps the given output stream.
     *
     * @param output non-null output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output) {
        return of(output, (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue());
    }

    /**
     * Wraps the given output stream.
     *
     * @param output     non-null output stream
     * @param bufferSize buffer size which is always greater than zero(usually 8192
     *                   or larger)
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output, int bufferSize) {
        return of(output, bufferSize, null);
    }

    /**
     * Wraps the given output stream.
     *
     * @param output     non-null output stream
     * @param bufferSize buffer size which is always greater than zero(usually 8192
     *                   or larger)
     * @param afterClose custom handler will be invoked right after closing the
     *                   output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output, int bufferSize, Runnable afterClose) {
        return output instanceof ClickHouseOutputStream ? (ClickHouseOutputStream) output
                : new WrappedOutputStream(output, bufferSize, afterClose);
    }

    protected final Runnable afterClose;

    protected boolean closed;

    protected ClickHouseOutputStream(Runnable afterClose) {
        this.afterClose = afterClose;
        this.closed = false;
    }

    @Override
    public final void write(int b) throws IOException {
        writeByte((byte) (0xFF & b));
    }

    @Override
    public final void write(byte[] b) throws IOException {
        writeBytes(b, 0, b.length);
    }

    @Override
    public final void write(byte[] b, int off, int len) throws IOException {
        writeBytes(b, off, len);
    }

    /**
     * Checks if the output stream has been closed or not.
     *
     * @return true if the output stream has been closed; false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            if (afterClose != null) {
                afterClose.run();
            }
        }
    }

    /**
     * Writes a single byte into output stream.
     *
     * @param b byte to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public abstract ClickHouseOutputStream writeByte(byte b) throws IOException;

    /**
     * Writes bytes into output stream.
     *
     * @param buffer non-null byte buffer
     * @param offset relative offset of the byte buffer
     * @param length bytes to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeBytes(ByteBuffer buffer, int offset, int length) throws IOException {
        if (buffer == null || offset < 0 || length < 0) {
            throw new IllegalArgumentException("Non-null ByteBuffer and positive offset and length are required");
        }

        byte[] bytes = new byte[length];
        // read-only ByteBuffer won't allow us to call its array() method for unwrapping
        buffer.get(bytes, offset, length);
        return writeBytes(bytes, 0, length);
    }

    /**
     * Writes bytes into output stream.
     *
     * @param bytes  non-null byte array
     * @param offset offset of the byte array
     * @param length bytes to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public abstract ClickHouseOutputStream writeBytes(byte[] bytes, int offset, int length) throws IOException;

    /**
     * Writes bytes into output stream.
     *
     * @param buffer wrapped byte array with offset and limit
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeBytes(ClickHouseByteBuffer buffer) throws IOException {
        if (buffer == null || buffer.isEmpty()) {
            return this;
        }

        return writeBytes(buffer.array(), buffer.position(), buffer.limit() - buffer.position());
    }

    /**
     * Writes string into the output stream. Nothing will happen when {@code value}
     * is
     * null or empty.
     *
     * @param value   string to write
     * @param charset charset, null is treated as {@link StandardCharsets#UTF_8}
     * @return this output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeString(String value, Charset charset) throws IOException {
        if (value == null || value.isEmpty()) {
            return writeByte((byte) 0);
        } else {
            byte[] bytes = value.getBytes(charset != null ? charset : StandardCharsets.UTF_8);
            int len = bytes.length;
            writeVarInt(len);
            return writeBytes(bytes, 0, len);
        }
    }

    /**
     * Writes ascii string into output stream. {@link #writeVarInt(int)} will be
     * called
     * automatically before writing the string.
     *
     * @param value ascii string to write
     * @return this output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeAsciiString(String value) throws IOException {
        return writeString(value, StandardCharsets.US_ASCII);
    }

    /**
     * Writes unicode string into output stream. {@link #writeVarInt(int)} will be
     * called
     * automatically before writing the string.
     *
     * @param value unicode string to write
     * @return this output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeUnicodeString(String value) throws IOException {
        return writeString(value, StandardCharsets.UTF_8);
    }

    /**
     * Writes varint into output stream.
     *
     * @param value varint
     * @return this output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeVarInt(int value) throws IOException {
        return writeUnsignedVarInt(value);
    }

    /**
     * Writes varint into output stream.
     *
     * @param value varint
     * @return this output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeUnsignedVarInt(long value) throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/abe314feecd1647d7c2b952a25da7abf5c19f352/src/IO/VarInt.h#L187
        int i = 0;
        for (; i < 9; i++) {
            byte b = (byte) (value & 0x7F);

            if (value > 0x7F) {
                b |= 0x80;
            }

            value >>= 7;
            writeByte(b);

            if (value == 0) {
                break;
            }
        }

        return this;
    }
}
