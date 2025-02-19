package com.clickhouse.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.clickhouse.data.stream.DeferredOutputStream;
import com.clickhouse.data.stream.EmptyOutputStream;
import com.clickhouse.data.stream.WrappedOutputStream;

/**
 * Extended output stream for write optimization. It also acts as a factory
 * class providing static methods for creating output stream as needed.
 */
@Deprecated
public abstract class ClickHouseOutputStream extends OutputStream {
    protected static final String ERROR_NULL_BYTES = "Non-null byte array is required";
    protected static final String ERROR_REUSE_BUFFER = "Please pass a different byte array instead of the same internal buffer for reading";
    protected static final String ERROR_STREAM_CLOSED = "Output stream has been closed";

    public static final String TYPE_NAME = "OutputStream";
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    /**
     * Wraps the given output stream.
     *
     * @param stream           pass-thru stream, could be null
     * @param output           non-null output stream
     * @param bufferSize       buffer size
     * @param compression      compression algorithm
     * @param compressionLevel compression level
     * @param postCloseAction  custom action will be performed right after closing
     *                         the wrapped output stream
     * @return non-null wrapped output stream
     */
    static ClickHouseOutputStream wrap(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
            ClickHouseCompression compression, int compressionLevel, Runnable postCloseAction) {
        return ClickHouseCompressionAlgorithm.createOutputStream(stream, output, bufferSize, compression,
                compressionLevel, postCloseAction);
    }

    /**
     * Gets an empty output stream that consumes nothing and cannot be closed.
     *
     * @return empty output stream
     */
    public static ClickHouseOutputStream empty() {
        return EmptyOutputStream.INSTANCE;
    }

    /**
     * Wraps the deferred output stream.
     *
     * @param deferredOutput  non-null deferred output stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped output
     */
    public static ClickHouseOutputStream of(ClickHouseDeferredValue<OutputStream> deferredOutput, int bufferSize,
            Runnable postCloseAction) {
        return new WrappedOutputStream(null, new DeferredOutputStream(deferredOutput), bufferSize, postCloseAction); // NOSONAR
    }

    /**
     * Wraps the given pass-thru stream as output stream.
     *
     * @param stream          non-null pass-thru stream
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @return wrapped output
     */
    public static ClickHouseOutputStream of(ClickHousePassThruStream stream, int bufferSize, Runnable postCloseAction) {
        if (stream == null || !stream.hasOutput()) {
            throw new IllegalArgumentException("Non-null pass-thru stream with output is required");
        }

        return stream.newOutputStream(bufferSize, postCloseAction);
    }

    /**
     * Wraps the given output stream.
     *
     * @param output output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output) {
        return of(output, DEFAULT_BUFFER_SIZE, null, -1, null);
    }

    /**
     * Wraps the given output stream.
     *
     * @param output     output stream
     * @param bufferSize buffer size which is always greater than zero(usually 4096
     *                   or larger)
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output, int bufferSize) {
        return of(output, bufferSize, null, -1, null);
    }

    /**
     * Wraps the given output stream.
     *
     * @param output          output stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param compression     compression algorithm, null or
     *                        {@link ClickHouseCompression#NONE} means no
     *                        compression
     * @param level           compression level
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return wrapped output, or the same output if it's instance of
     *         {@link ClickHouseOutputStream}
     */
    public static ClickHouseOutputStream of(OutputStream output, int bufferSize, ClickHouseCompression compression,
            int level, Runnable postCloseAction) {
        final ClickHouseOutputStream chOutput;
        if (output == null) {
            chOutput = EmptyOutputStream.INSTANCE;
        } else if (compression == null || compression == ClickHouseCompression.NONE) {
            chOutput = output != EmptyOutputStream.INSTANCE && output instanceof ClickHouseOutputStream
                    ? (ClickHouseOutputStream) output
                    : new WrappedOutputStream(null, output, bufferSize, postCloseAction);
        } else {
            chOutput = wrap(null, output, bufferSize, compression, level, postCloseAction);
        }
        return chOutput;
    }

    protected final ClickHousePassThruStream stream;
    protected final Runnable postCloseAction;

    protected volatile boolean closed;

    protected ClickHouseOutputStream(ClickHousePassThruStream stream, Runnable postCloseAction) {
        this.stream = stream != null ? stream : ClickHousePassThruStream.NULL;
        this.postCloseAction = postCloseAction;
        this.closed = false;
    }

    protected void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException(ERROR_STREAM_CLOSED);
        }
    }

    /**
     * Gets underlying file.
     *
     * @return non-null underlying file
     */
    public ClickHouseFile getUnderlyingFile() {
        return stream instanceof ClickHouseFile ? ((ClickHouseFile) stream) : ClickHouseFile.NULL;
    }

    /**
     * Gets underlying stream.
     *
     * @return non-null underlying stream
     */
    public ClickHousePassThruStream getUnderlyingStream() {
        return stream;
    }

    /**
     * Checks if there's underlying output stream. Same as
     * {@code getUnderlyingStream().hasOutput()}.
     *
     * @return true if there's underlying output stream; false otherwise
     */
    public boolean hasUnderlyingStream() {
        return stream.hasOutput();
    }

    /**
     * Transfers bytes into output stream without creating a copy.
     *
     * @param bytes non-null byte array
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or operate on a closed stream
     * @deprecated will be dropped in 0.5, please use
     *             {@link #writeBuffer(ClickHouseByteBuffer)} instead
     */
    @Deprecated
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
     *                     to sent all bytes, or operate on a closed stream
     * @deprecated will be dropped in 0.5, please use
     *             {@link #writeBuffer(ClickHouseByteBuffer)} instead
     */
    @Deprecated
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
     *                     to sent all bytes, or operate on a closed stream
     * @deprecated will be dropped in 0.5, please use
     *             {@link #writeBuffer(ClickHouseByteBuffer)} instead
     */
    @Deprecated
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
            ClickHouseDataStreamFactory.handleCustomAction(postCloseAction);
        }
    }

    /**
     * Writes a byte (true=1, false=0) into output stream.
     *
     * @param b boolean value to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or operate on a closed stream
     */
    public ClickHouseOutputStream writeBoolean(boolean b) throws IOException {
        return writeByte(b ? (byte) 1 : (byte) 0);
    }

    /**
     * Writes a single byte into output stream.
     *
     * @param b byte to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or operate on a closed stream
     */
    public abstract ClickHouseOutputStream writeByte(byte b) throws IOException;

    /**
     * Writes bytes into output stream.
     *
     * @param buffer non-null byte buffer
     * @param length bytes to write
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
     */
    public abstract ClickHouseOutputStream writeBytes(byte[] bytes, int offset, int length) throws IOException;

    /**
     * Writes bytes into output stream. Unlike
     * {@link #writeBytes(byte[], int, int)}, which always writes bytes into a
     * fixed-size buffer first, this may skip the internal buffer for less memory
     * footprint and better performance.
     *
     * @param buffer wrapped byte array with offset and limit
     * @return current output stream
     * @throws IOException when failed to write value into output stream, not able
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
     *                     to sent all bytes, or operate on a closed stream
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
