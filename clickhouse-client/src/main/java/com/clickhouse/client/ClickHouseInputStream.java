package com.clickhouse.client;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.stream.BlockingInputStream;
import com.clickhouse.client.stream.DeferredInputStream;
import com.clickhouse.client.stream.EmptyInputStream;
import com.clickhouse.client.stream.Lz4InputStream;
import com.clickhouse.client.stream.IterableByteArrayInputStream;
import com.clickhouse.client.stream.IterableByteBufferInputStream;
import com.clickhouse.client.stream.IterableMultipleInputStream;
import com.clickhouse.client.stream.IterableObjectInputStream;
import com.clickhouse.client.stream.WrappedInputStream;

/**
 * Extended input stream for read optimization. Methods like
 * {@link #readBuffer(int)}, {@link #readByte()}, {@link #readBytes(int)}, and
 * {@link #readCustom(ClickHouseDataUpdater)} are added to reduce object
 * creation as well as closing the stream when it reaches end of stream. This
 * class is also responsible for creating various input stream as needed.
 */
public abstract class ClickHouseInputStream extends InputStream {
    /**
     * Empty byte array.
     *
     * @deprecated will be removed in v0.3.3, please use
     *             {@link ClickHouseByteBuffer#EMPTY_BYTES} instead
     */
    @Deprecated
    public static final byte[] EMPTY_BYTES = ClickHouseByteBuffer.EMPTY_BYTES;
    /**
     * Empty and read-only byte buffer.
     *
     * @deprecated will be removed in v0.3.3, please use
     *             {@link ClickHouseByteBuffer#EMPTY_BUFFER} instead
     */
    @Deprecated
    public static final ByteBuffer EMPTY_BUFFER = ClickHouseByteBuffer.EMPTY_BUFFER;

    protected static final String ERROR_INCOMPLETE_READ = "Reached end of input stream after reading %d of %d bytes";
    protected static final String ERROR_NULL_BYTES = "Non-null byte array is required";
    protected static final String ERROR_REUSE_BUFFER = "Please pass a different byte array instead of the same internal buffer for reading";
    protected static final String ERROR_STREAM_CLOSED = "Input stream has been closed";

    /**
     * Gets an empty input stream that produces nothing and cannot be closed.
     *
     * @return empty input stream
     */
    public static ClickHouseInputStream empty() {
        return EmptyInputStream.INSTANCE;
    }

    /**
     * Wraps the given blocking queue.
     *
     * @param queue   non-null blocking queue
     * @param timeout read timeout in milliseconds
     * @return wrapped input
     */
    public static ClickHouseInputStream of(BlockingQueue<ByteBuffer> queue, int timeout) {
        return of(queue, timeout, null);
    }

    /**
     * Wraps the given blocking queue.
     *
     * @param queue           non-null blocking queue
     * @param timeout         read timeout in milliseconds
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(BlockingQueue<ByteBuffer> queue, int timeout, Runnable postCloseAction) {
        return new BlockingInputStream(queue, timeout, postCloseAction);
    }

    /**
     * Wraps the deferred input stream.
     *
     * @param deferredInput   non-null deferred input stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHouseDeferredValue<InputStream> deferredInput, int bufferSize,
            Runnable postCloseAction) {
        return new WrappedInputStream(new DeferredInputStream(deferredInput), bufferSize, postCloseAction);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input      input stream
     * @param bufferSize buffer size which is always greater than zero(usually 4096
     *                   or larger)
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize) {
        return of(input, bufferSize, null, null);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input           input stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize, Runnable postCloseAction) {
        return of(input, bufferSize, null, postCloseAction);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input       input stream
     * @param bufferSize  buffer size which is always greater than zero(usually 4096
     *                    or larger)
     * @param compression compression algorithm, null or
     *                    {@link ClickHouseCompression#NONE} means no compression
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize, ClickHouseCompression compression) {
        return of(input, bufferSize, compression, null);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input           input stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param compression     compression algorithm, null or
     *                        {@link ClickHouseCompression#NONE} means no
     *                        compression
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize, ClickHouseCompression compression,
            Runnable postCloseAction) {
        if (input == null) {
            return EmptyInputStream.INSTANCE;
        }

        ClickHouseInputStream chInput;
        if (compression != null && compression != ClickHouseCompression.NONE) {
            switch (compression) {
                case GZIP:
                    try {
                        chInput = new WrappedInputStream(new GZIPInputStream(input), bufferSize, postCloseAction);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to wrap input stream", e);
                    }
                    break;
                case LZ4:
                    chInput = new Lz4InputStream(input, postCloseAction);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported compression algorithm: " + compression);
            }
        } else {
            chInput = input instanceof ClickHouseInputStream ? (ClickHouseInputStream) input
                    : new WrappedInputStream(input, bufferSize, postCloseAction);
        }

        return chInput;
    }

    /**
     * Wraps the given byte arrays.
     *
     * @param bytes array of byte array
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static ClickHouseInputStream of(byte[]... bytes) {
        if (bytes == null || bytes.length == 0) {
            return EmptyInputStream.INSTANCE;
        }
        return new IterableByteArrayInputStream(Arrays.asList(bytes), null);
    }

    /**
     * Wraps the given byte buffers.
     *
     * @param buffers array of byte buffer
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static ClickHouseInputStream of(ByteBuffer... buffers) {
        if (buffers == null || buffers.length == 0) {
            return EmptyInputStream.INSTANCE;
        }
        return new IterableByteBufferInputStream(Arrays.asList(buffers), null);
    }

    /**
     * Wraps the given files.
     *
     * @param files array of file
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static ClickHouseInputStream of(File... files) {
        if (files == null || files.length == 0) {
            return EmptyInputStream.INSTANCE;
        }
        return of(Arrays.asList(files), File.class, null, null);
    }

    /**
     * Wraps the given input streams.
     *
     * @param inputs array of input stream
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static ClickHouseInputStream of(InputStream... inputs) {
        if (inputs == null || inputs.length == 0) {
            return EmptyInputStream.INSTANCE;
        } else if (inputs.length == 1) {
            return of(inputs[0], (int) ClickHouseClientOption.READ_BUFFER_SIZE.getDefaultValue(), null, null);
        }
        return of(Arrays.asList(inputs), InputStream.class, null, null);
    }

    /**
     * Wraps the given (UTF-8)strings.
     *
     * @param strings array of string
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static ClickHouseInputStream of(String... strings) {
        if (strings == null || strings.length == 0) {
            return EmptyInputStream.INSTANCE;
        }
        return of(Arrays.asList(strings), String.class, null, null);
    }

    /**
     * Wraps the given URLs.
     *
     * @param urls array of URL
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static ClickHouseInputStream of(URL... urls) {
        if (urls == null || urls.length == 0) {
            return EmptyInputStream.INSTANCE;
        }
        return of(Arrays.asList(urls), URL.class, null, null);
    }

    /**
     * Wraps the given array of object as byte array based binary input stream.
     *
     * @param <T>             type of the object
     * @param source          array of object
     * @param clazz           class of the object
     * @param converter       optional transformer to convert each object into byte
     *                        array
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return non-null input stream
     * @see #of(Iterable, Class, Function, Runnable)
     */
    public static <T> ClickHouseInputStream of(T[] source, Class<T> clazz, Function<T, byte[]> converter,
            Runnable postCloseAction) {
        if (source == null || source.length == 0) {
            return EmptyInputStream.INSTANCE;
        }
        return of(Arrays.asList(source), clazz, converter, postCloseAction);
    }

    /**
     * Wraps the given iterable objects as byte array based binary input stream.
     * {@code byte[]}, {@link ByteBuffer}, {@link InputStream}, {@link File},
     * {@link String}, and {@link URL} are all supported by default.
     *
     * @param <T>             type of the object
     * @param source          iterable objects(e.g. byte[], ByteBuffer, and
     *                        String etc.)
     * @param clazz           class of the object
     * @param converter       optional transformer to convert each object into byte
     *                        array
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return non-null input stream
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseInputStream of(Iterable<T> source, Class<T> clazz, Function<T, byte[]> converter,
            Runnable postCloseAction) {
        if (source == null) {
            return EmptyInputStream.INSTANCE;
        } else if (converter != null) { // respect custom converter regardless object type
            return new IterableObjectInputStream<>(source, converter, postCloseAction);
        } else if (clazz == byte[].class) {
            return new IterableByteArrayInputStream((Iterable<byte[]>) source, postCloseAction);
        } else if (clazz == ByteBuffer.class) {
            return new IterableByteBufferInputStream((Iterable<ByteBuffer>) source, postCloseAction);
        } else if (clazz == File.class) { // Too many dependencies if we consider VFS(FileObject and FileContent)
            return new IterableMultipleInputStream<>((Iterable<File>) source, f -> {
                if (f == null) {
                    return null;
                }
                try {
                    // TODO decompress as needed ClickHouseCompression.fromFileName(f.getName())
                    return new FileInputStream(f);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }, postCloseAction);
        } else if (clazz == String.class) {
            return new IterableObjectInputStream<>((Iterable<String>) source,
                    s -> s == null || s.isEmpty() ? ClickHouseByteBuffer.EMPTY_BYTES
                            : s.getBytes(StandardCharsets.UTF_8),
                    postCloseAction);
        } else if (clazz == URL.class) {
            return new IterableMultipleInputStream<>((Iterable<URL>) source, u -> {
                if (u == null) {
                    return null;
                }
                try {
                    return u.openStream();
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }, postCloseAction);
        } else if (InputStream.class.isAssignableFrom(clazz)) {
            return new IterableMultipleInputStream<>((Iterable<InputStream>) source, i -> i, postCloseAction);
        } else {
            throw new IllegalArgumentException("Missing converter for data type: " + clazz);
        }
    }

    /**
     * Pipes data from input stream to output stream. Input stream will be closed
     * but output stream will remain open.
     *
     * @param input      non-null input stream, which will be closed
     * @param output     non-null output stream, which will remain open
     * @param bufferSize buffer size, zero or negative number will be treated as
     *                   {@link ClickHouseClientOption#WRITE_BUFFER_SIZE}
     * @return written bytes
     * @throws IOException when error occured reading from input stream or writing
     *                     data to output stream
     */
    public static long pipe(InputStream input, OutputStream output, int bufferSize) throws IOException {
        bufferSize = ClickHouseUtils.getBufferSize(bufferSize,
                (int) ClickHouseClientOption.WRITE_BUFFER_SIZE.getDefaultValue(),
                (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue());
        return pipe(input, output, new byte[bufferSize]);
    }

    /**
     * Pipes data from input stream to output stream. Input stream will be closed
     * but output stream will remain open.
     *
     * @param input  non-null input stream, which will be closed
     * @param output non-null output stream, which will remain open
     * @param buffer non-empty buffer
     * @return written bytes
     * @throws IOException when error occured reading from input stream or writing
     *                     data to output stream
     */
    public static long pipe(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        if (input == null || output == null || buffer == null || buffer.length < 1) {
            throw new IllegalArgumentException("Non-null input, output, and write buffer are required");
        }

        int size = buffer.length;
        long count = 0;
        int written = 0;
        try {
            while ((written = input.read(buffer, 0, size)) >= 0) {
                output.write(buffer, 0, written);
                count += written;
            }
            output.flush();
            input.close();
            input = null;
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        return count;
    }

    /**
     * Non-null reusable byte buffer.
     */
    protected final ClickHouseByteBuffer byteBuffer;
    /**
     * Optional post close action.
     */
    protected final Runnable postCloseAction;

    protected boolean closed;

    protected ClickHouseInputStream(Runnable postCloseAction) {
        this.byteBuffer = ClickHouseByteBuffer.newInstance();
        this.postCloseAction = postCloseAction;

        this.closed = false;
    }

    /**
     * Checks whether the input stream has been closed and throw an exception if it
     * is.
     * 
     * @throws IOException when the input stream has been closed
     */
    protected void ensureOpen() throws IOException {
        if (this.closed) {
            throw new IOException(ERROR_STREAM_CLOSED);
        }
    }

    /**
     * Closes the input stream quietly.
     */
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
     * Reads all remaining bytes and write into given output stream. Current input
     * stream will be closed automatically at the end of writing, but {@code output}
     * will remain open.
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
     * Reads byte buffer from the input stream.
     *
     * @param length byte length
     * @return non-null byte buffer
     * @throws IOException when failed to read bytes from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public ClickHouseByteBuffer readBuffer(int length) throws IOException {
        if (length < 1) {
            return byteBuffer.reset();
        }

        return byteBuffer.update(readBytes(length));
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
        if (length < 1) {
            return ClickHouseByteBuffer.EMPTY_BYTES;
        }
        ensureOpen();

        byte[] bytes = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = read(bytes, offset, length - offset);
            if (read == -1) {
                closeQuietly();
                throw offset == 0 ? new EOFException()
                        : new IOException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, offset, length));
            } else {
                offset += read;
            }
        }

        return bytes;
    }

    /**
     * Reads bytes using custom reader. Stream will be closed automatically when it
     * reached end of stream. However, unlike {@link #readBuffer(int)}, this method
     * will never throw {@link EOFException}.
     *
     * @param reader non-null data reader
     * @return non-null byte buffer
     * @throws IOException when failed to read bytes from input stream, not able to
     *                     retrieve all bytes, or reached end of the stream
     */
    public abstract ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException;

    /**
     * Reads binary string from the input stream. {@link #readVarInt()} will be
     * first called automatically to understand byte length of the string.
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
     * Reads binary string from the input stream. When {@code byteLength} is zero or
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

        return readBuffer(byteLength).asString(charset);
    }

    /**
     * Reads ascii string from input stream. {@link #readVarInt()} will be first
     * called automatically to understand byte length of the string.
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
            if (postCloseAction != null) {
                postCloseAction.run();
            }
        }
    }
}
