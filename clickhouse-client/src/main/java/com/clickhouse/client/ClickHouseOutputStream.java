package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.stream.Lz4OutputStream;
import com.clickhouse.client.stream.WrappedOutputStream;

/**
 * Extended output stream for write optimization. It also acts as a factory
 * class providing static methods for creating output stream as needed.
 */
public abstract class ClickHouseOutputStream extends OutputStream {
    protected static final String ERROR_INCOMPLETE_READ = "Reached end of input stream after reading %d of %d bytes";
    protected static final String ERROR_NULL_BYTES = "Non-null byte array is required";
    protected static final String ERROR_REUSE_BUFFER = "Please pass a different byte array instead of the same internal buffer for reading";
    protected static final String ERROR_STREAM_CLOSED = "Output stream has been closed";

    /**
     * Wraps the given output stream.
     *
     * @param output non-null output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output) {
        return of(output, (int) ClickHouseClientOption.WRITE_BUFFER_SIZE.getDefaultValue(), null, null);
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
        return of(output, bufferSize, null, null);
    }

    /**
     * Wraps the given output stream.
     *
     * @param output          non-null output stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        8192 or larger)
     * @param compression     compression algorithm, null or
     *                        {@link ClickHouseCompression#NONE} means no
     *                        compression
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output, int bufferSize, ClickHouseCompression compression,
            Runnable postCloseAction) {
        ClickHouseOutputStream chOutput;
        if (compression != null && compression != ClickHouseCompression.NONE) {
            switch (compression) {
                case GZIP:
                    try {
                        chOutput = new WrappedOutputStream(new GZIPOutputStream(output), bufferSize, postCloseAction);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to wrap input stream", e);
                    }
                    break;
                case LZ4:
                    chOutput = new Lz4OutputStream(output, bufferSize, postCloseAction);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported compression algorithm: " + compression);
            }
        } else {
            chOutput = output instanceof ClickHouseOutputStream ? (ClickHouseOutputStream) output
                    : new WrappedOutputStream(output, bufferSize, postCloseAction);
        }

        return chOutput;
    }

    protected final Runnable postCloseAction;

    protected boolean closed;

    protected ClickHouseOutputStream(Runnable postCloseAction) {
        this.postCloseAction = postCloseAction;
        this.closed = false;
    }

    protected void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException(ERROR_STREAM_CLOSED);
        }
    }

    /**
     * Transfers bytes into output stream without creating a copy.
     *
     * @param bytes non-null byte array
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public final ClickHouseOutputStream transferBytes(byte[] bytes) throws IOException {
        return transferBytes(bytes, 0, bytes.length);
    }

    /**
     * Transfer bytes into output stream without creating a copy.
     *
     * @param buffer non-null byte buffer
     * @param length bytes to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream transferBytes(ByteBuffer buffer, int length) throws IOException {
        if (buffer == null || length < 0) {
            throw new IllegalArgumentException("Non-null ByteBuffer and positive length are required");
        }

        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[length];
            buffer.get(bytes);
        }
        return transferBytes(bytes, 0, length);
    }

    /**
     * Transfers the given bytes into output stream, without creating a copy as in
     * {@link #writeBytes(byte[], int, int)}.
     *
     * @param bytes  non-null byte array
     * @param offset offset of the byte array
     * @param length bytes to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public abstract ClickHouseOutputStream transferBytes(byte[] bytes, int offset, int length) throws IOException;

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
        if (closed) {
            return;
        }

        try {
            flush();
        } finally {
            closed = true;
            if (postCloseAction != null) {
                postCloseAction.run();
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
     * @param length bytes to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public ClickHouseOutputStream writeBytes(ByteBuffer buffer, int length) throws IOException {
        if (buffer == null || length < 0) {
            throw new IllegalArgumentException("Non-null ByteBuffer and positive length are required");
        }

        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[length];
            buffer.get(bytes);
        }
        return writeBytes(bytes, 0, length);
    }

    /**
     * Writes bytes into output stream.
     *
     * @param bytes non-null byte array
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public final ClickHouseOutputStream writeBytes(byte[] bytes) throws IOException {
        return writeBytes(bytes, 0, bytes.length);
    }

    /**
     * Writes copy of given bytes into output stream.
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
    public ClickHouseOutputStream writeBuffer(ClickHouseByteBuffer buffer) throws IOException {
        if (buffer == null || buffer.isEmpty()) {
            return this;
        }
        return writeBytes(buffer.array(), buffer.position(), buffer.length());
    }

    /**
     * Writes bytes using custom writer.
     *
     * @param writer non-null data writer
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or opereate on a closed stream
     */
    public abstract ClickHouseOutputStream writeCustom(ClickHouseDataUpdater writer) throws IOException;

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
