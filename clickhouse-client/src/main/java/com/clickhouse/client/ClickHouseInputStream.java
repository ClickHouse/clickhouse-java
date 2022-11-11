package com.clickhouse.client;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    protected static final String ERROR_INCOMPLETE_READ = "Reached end of input stream after reading %d of %d bytes";
    protected static final String ERROR_NULL_BYTES = "Non-null byte array is required";
    protected static final String ERROR_REUSE_BUFFER = "Please pass a different byte array instead of the same internal buffer for reading";
    protected static final String ERROR_STREAM_CLOSED = "Input stream has been closed";

    /**
     * Wraps the given input stream.
     *
     * @param file             wrapped file, could be null
     * @param input            non-null input stream
     * @param bufferSize       buffer size
     * @param postCloseAction  custom action will be performed right after closing
     *                         the wrapped input stream
     * @param compression      compression algorithm
     * @param compressionLevel compression level
     * @return non-null wrapped input stream
     */
    public static ClickHouseInputStream wrap(ClickHouseFile file, InputStream input, int bufferSize,
            Runnable postCloseAction, ClickHouseCompression compression, int compressionLevel) {
        final ClickHouseInputStream chInput;
        if (compression == null || compression == ClickHouseCompression.NONE) {
            chInput = input != EmptyInputStream.INSTANCE && input instanceof ClickHouseInputStream
                    ? (ClickHouseInputStream) input
                    : new WrappedInputStream(file, input, bufferSize, postCloseAction);
        } else {
            switch (compression) {
                case GZIP:
                    try {
                        chInput = new WrappedInputStream(file, new GZIPInputStream(input), bufferSize, postCloseAction);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to wrap input stream", e);
                    }
                    break;
                case LZ4:
                    chInput = new Lz4InputStream(file, input, postCloseAction);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported compression algorithm: " + compression);
            }
        }
        return chInput;
    }

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
     *                        8192 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHouseDeferredValue<InputStream> deferredInput, int bufferSize,
            Runnable postCloseAction) {
        return new WrappedInputStream(null, new DeferredInputStream(deferredInput), bufferSize, postCloseAction); // NOSONAR
    }

    /**
     * Wraps the given file as input stream.
     *
     * @param file            non-null file
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        8192 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHouseFile file, int bufferSize, Runnable postCloseAction) {
        if (file == null || !file.isAvailable()) {
            throw new IllegalArgumentException("Non-null file required");
        }
        try {
            return wrap(file, new FileInputStream(file.getFile()), bufferSize, postCloseAction,
                    ClickHouseCompression.NONE, file.getCompressionLevel());
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Wraps the given input stream.
     *
     * @param input      input stream
     * @param bufferSize buffer size which is always greater than zero(usually 8192
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
     *                        8192 or larger)
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
     * @param bufferSize  buffer size which is always greater than zero(usually 8192
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
     *                        8192 or larger)
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
        } else if (input != EmptyInputStream.INSTANCE && input instanceof ClickHouseInputStream) {
            return (ClickHouseInputStream) input;
        }
        return wrap(null, input, bufferSize, postCloseAction, compression, 0);
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
            return of(inputs[0], (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(), null, null);
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
     * Transfers data from input stream to output stream. Input stream will be
     * closed but output stream will remain open. Please pay attention that you need
     * to explictly call {@code output.flush()} before closing output stream.
     *
     * @param input      non-null input stream, which will be closed
     * @param output     non-null output stream, which will remain open
     * @param bufferSize buffer size, zero or negative number will be treated as
     *                   {@link ClickHouseClientOption#BUFFER_SIZE}
     * @return written bytes
     * @throws IOException when error occured reading from input stream or writing
     *                     data to output stream
     */
    public static long pipe(InputStream input, OutputStream output, int bufferSize) throws IOException {
        if (input instanceof ClickHouseInputStream && output instanceof ClickHouseOutputStream) {
            return ((ClickHouseInputStream) input).pipe((ClickHouseOutputStream) output);
        }

        bufferSize = ClickHouseUtils.getBufferSize(bufferSize,
                (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue());
        return pipe(input, output, new byte[bufferSize]);
    }

    /**
     * Transfers data from input stream to output stream. Input stream will be
     * closed but output stream will remain open. Please pay attention that you need
     * to explictly call {@code output.flush()} before closing output stream.
     *
     * @param input  non-null input stream, which will be closed
     * @param output non-null output stream, which will remain open
     * @param buffer non-empty buffer
     * @return written bytes
     * @throws IOException when error occured reading from input stream or writing
     *                     data to output stream
     */
    public static long pipe(InputStream input, OutputStream output, byte[] buffer) throws IOException {
        if (buffer == null && input instanceof ClickHouseInputStream && output instanceof ClickHouseOutputStream) {
            return ((ClickHouseInputStream) input).pipe((ClickHouseOutputStream) output);
        } else if (input == null || output == null || buffer == null || buffer.length < 1) {
            throw new IllegalArgumentException("Non-null input, output, and write buffer are required");
        }

        int size = buffer.length;
        long count = 0L;
        int written = 0;
        try (InputStream in = input) {
            while ((written = in.read(buffer, 0, size)) >= 0) {
                output.write(buffer, 0, written);
                count += written;
            }
        }
        return count;
    }

    public static File save(InputStream in, int bufferSize, int timeout) {
        return save(null, in, bufferSize, timeout, true);
    }

    public static File save(File file, InputStream in, int bufferSize, int timeout, boolean deleteOnExit) {
        final File tmp;
        if (file != null) {
            tmp = file;
            if (deleteOnExit) {
                tmp.deleteOnExit();
            }
        } else {
            try {
                tmp = File.createTempFile("chc", "data"); // NOSONAR
                tmp.deleteOnExit();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create temp file", e);
            }
        }
        CompletableFuture<File> data = CompletableFuture.supplyAsync(() -> {
            try {
                try (OutputStream out = new FileOutputStream(tmp)) {
                    pipe(in, out, bufferSize);
                }
                return tmp;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        try {
            return data.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UncheckedIOException) {
                throw ((UncheckedIOException) cause);
            } else if (cause instanceof IOException) {
                throw new UncheckedIOException((IOException) cause);
            }
            throw new IllegalStateException(cause);
        }
    }

    /**
     * Non-null reusable byte buffer.
     */
    protected final ClickHouseByteBuffer byteBuffer;
    /**
     * Underlying file.
     */
    protected final ClickHouseFile file;
    /**
     * Optional post close action.
     */
    protected final Runnable postCloseAction;

    protected boolean closed;
    protected OutputStream copyTo;

    protected ClickHouseInputStream(ClickHouseFile file, OutputStream copyTo, Runnable postCloseAction) {
        this.byteBuffer = ClickHouseByteBuffer.newInstance();
        this.file = file != null ? file : ClickHouseFile.NULL;
        this.postCloseAction = postCloseAction;

        this.closed = false;
        this.copyTo = copyTo;
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
     * Gets underlying file.
     *
     * @return non-null underlying file
     */
    public ClickHouseFile getUnderlyingFile() {
        return file;
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
    public short readUnsignedByte() throws IOException {
        return (short) (0xFF & readByte());
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
     * Reads a byte as boolean. The byte value can be either 0 (false) or 1 (true).
     *
     * @return boolean value
     * @throws IOException when failed to read boolean value from input stream or
     *                     reached end of the stream
     */
    public boolean readBoolean() throws IOException {
        byte b = readByte();
        if (b == (byte) 0) {
            return false;
        } else if (b == (byte) 1) {
            return true;
        } else {
            throw new IOException("Failed to read boolean value, expect 0 (false) or 1 (true) but we got: " + b);
        }
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
     * Reads a varint from input stream.
     *
     * @return varint
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public int readVarInt() throws IOException {
        // https://github.com/ClickHouse/ClickHouse/blob/abe314feecd1647d7c2b952a25da7abf5c19f352/src/IO/VarInt.h#L126
        int b = readByte();
        if (b >= 0) {
            return b;
        }

        int result = b & 0x7F;
        for (int shift = 7; shift <= 28; shift += 7) {
            if ((b = readByte()) >= 0) {
                result |= b << shift;
                break;
            } else {
                result |= (b & 0x7F) << shift;
            }
        }
        // consume a few more bytes - readVarLong() should be called instead
        if (b < 0) {
            for (int shift = 35; shift <= 63; shift += 7) {
                if (peek() < 0 || readByte() >= 0) {
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Reads 64-bit varint as long from input stream.
     *
     * @return 64-bit varint
     * @throws IOException when failed to read value from input stream or reached
     *                     end of the stream
     */
    public long readVarLong() throws IOException {
        long b = readByte();
        if (b >= 0L) {
            return b;
        }

        long result = b & 0x7F;
        for (int shift = 7; shift <= 63; shift += 7) {
            if ((b = readByte()) >= 0) {
                result |= b << shift;
                break;
            } else {
                result |= (b & 0x7F) << shift;
            }
        }
        return result;
    }

    /**
     * Sets target output stream to copy bytes to. This is mainly used for
     * testing, for example: dump input into a file while reading.
     *
     * @param out the output stream to write bytes to
     * @throws IOException when failed to flush previous target or not able to write
     *                     remaining bytes in buffer to the given output stream
     */
    public final void setCopyToTarget(OutputStream out) throws IOException {
        if (this.copyTo != null) {
            this.copyTo.flush();
        } else if (out != null) {
            // process remaining bytes in current buffer
            readCustom((b, p, l) -> {
                if (p < l) {
                    out.write(b, p, l - p);
                }
                return 0;
            });
        }
        this.copyTo = out;
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
