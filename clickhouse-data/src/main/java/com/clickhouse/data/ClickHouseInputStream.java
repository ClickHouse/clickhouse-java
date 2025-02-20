package com.clickhouse.data;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.clickhouse.data.stream.BlockingInputStream;
import com.clickhouse.data.stream.DeferredInputStream;
import com.clickhouse.data.stream.EmptyInputStream;
import com.clickhouse.data.stream.RestrictedInputStream;
import com.clickhouse.data.stream.IterableByteArrayInputStream;
import com.clickhouse.data.stream.IterableByteBufferInputStream;
import com.clickhouse.data.stream.IterableMultipleInputStream;
import com.clickhouse.data.stream.IterableObjectInputStream;
import com.clickhouse.data.stream.WrappedInputStream;

/**
 * Extended input stream for read optimization. Methods like
 * {@link #readBuffer(int)}, {@link #readByte()}, {@link #readBytes(int)}, and
 * {@link #readCustom(ClickHouseDataUpdater)} are added to reduce object
 * creation as well as closing the stream when it reaches end of stream. This
 * class is also responsible for creating various input stream as needed.
 */
@Deprecated
public abstract class ClickHouseInputStream extends InputStream implements Iterable<ClickHouseByteBuffer> {
    protected static final String ERROR_INCOMPLETE_READ = "Reached end of input stream after reading %d of %d bytes";
    protected static final String ERROR_NULL_BYTES = "Non-null byte array is required";
    protected static final String ERROR_REUSE_BUFFER = "Please pass a different byte array instead of the same internal buffer for reading";
    protected static final String ERROR_STREAM_CLOSED = "Input stream has been closed";

    public static final String TYPE_NAME = "InputStream";

    static class ByteBufferIterator implements Iterator<ClickHouseByteBuffer> {
        private final ClickHouseInputStream input;

        private ByteBufferIterator(ClickHouseInputStream input) {
            this.input = input;
        }

        @Override
        public boolean hasNext() {
            try {
                return input.available() > 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ClickHouseByteBuffer next() {
            try {
                ClickHouseByteBuffer buffer = input.nextBuffer();
                if (buffer.isEmpty() && input.available() < 1) {
                    throw new NoSuchElementException(
                            "No more byte buffer for read as we reached end of the stream");
                }
                return buffer;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Wraps the given input stream.
     *
     * @param stream           pass-thru stream, could be null
     * @param input            non-null input stream
     * @param bufferSize       buffer size
     * @param compression      compression algorithm
     * @param compressionLevel compression level
     * @param postCloseAction  custom action will be performed right after closing
     *                         the wrapped input stream
     * @return non-null wrapped input stream
     */
    public static ClickHouseInputStream wrap(ClickHousePassThruStream stream, InputStream input, int bufferSize,
            ClickHouseCompression compression, int compressionLevel, Runnable postCloseAction) {
        return ClickHouseCompressionAlgorithm.createInputStream(stream, input, bufferSize, compression,
                compressionLevel, postCloseAction);
    }

    /**
     * Wraps the given input stream with length limitation. Please pay attention
     * that calling close() method of the wrapper will never close the inner input
     * stream.
     *
     * @param input           non-null input stream
     * @param bufferSize      buffer size
     * @param length          maximum bytes can be read from the input
     * @param postCloseAction custom action will be performed right after closing
     *                        the wrapped input stream
     * @return non-null wrapped input stream
     */
    public static ClickHouseInputStream wrap(InputStream input, int bufferSize, long length, Runnable postCloseAction) {
        if (input instanceof RestrictedInputStream) {
            RestrictedInputStream ris = (RestrictedInputStream) input;
            if (ris.getRemaining() == length) {
                return ris;
            }
        }

        return new RestrictedInputStream(null, input, bufferSize, length, postCloseAction);
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
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHouseDeferredValue<InputStream> deferredInput, int bufferSize,
            Runnable postCloseAction) {
        return new WrappedInputStream(null, new DeferredInputStream(deferredInput), bufferSize, postCloseAction); // NOSONAR
    }

    /**
     * Wraps the given pass-thru stream as input stream.
     *
     * @param stream          non-null pass-thru stream
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHousePassThruStream stream, int bufferSize, Runnable postCloseAction) {
        if (stream == null || !stream.hasInput()) {
            throw new IllegalArgumentException("Non-null pass-thru stream with input is required");
        }

        return stream.newInputStream(bufferSize, postCloseAction);
    }

    /**
     * Creates an input stream using the given customer writer. Behind the scene, a
     * piped stream will be created, writer will be called in a separate worker
     * thread for writing.
     *
     * @param writer non-null customer writer
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHouseWriter writer) {
        return new DelegatedInputStream(null, writer);
    }

    /**
     * Creates an input stream using the given customer writer. Behind the scene, a
     * piped stream will be created, writer will be called in a separate worker
     * thread for writing.
     *
     * @param config configuration, could be null
     * @param writer non-null customer writer
     * @return wrapped input
     */
    public static ClickHouseInputStream of(ClickHouseDataConfig config, ClickHouseWriter writer) {
        return new DelegatedInputStream(config, writer);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input) {
        return of(input, ClickHouseDataConfig.getDefaultReadBufferSize(), null,
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, null);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input      input stream
     * @param bufferSize buffer size which is always greater than zero(usually
     *                   4096 or larger)
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize) {
        return of(input, ClickHouseDataConfig.getBufferSize(bufferSize), null,
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, null);
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
        return of(input, bufferSize, ClickHouseCompression.NONE, ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL,
                postCloseAction);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input       input stream
     * @param compression compression algorithm, null or
     *                    {@link ClickHouseCompression#NONE} means no compression
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, ClickHouseCompression compression) {
        return of(input, ClickHouseDataConfig.getDefaultReadBufferSize(), compression,
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, null);
    }

    /**
     * Wraps the given input stream.
     *
     * @param input            input stream
     * @param bufferSize       buffer size which is always greater than zero(usually
     *                         4096 or larger)
     * @param compression      compression algorithm, null or
     *                         {@link ClickHouseCompression#NONE} means no
     *                         compression
     * @param compressionLevel compression level
     * @param postCloseAction  custom action will be performed right after closing
     *                         the input stream
     * @return wrapped input, or the same input if it's instance of
     *         {@link ClickHouseInputStream}
     */
    public static ClickHouseInputStream of(InputStream input, int bufferSize, ClickHouseCompression compression,
            int compressionLevel, Runnable postCloseAction) {
        if (input == null) {
            return EmptyInputStream.INSTANCE;
        } else if (input != EmptyInputStream.INSTANCE && input instanceof ClickHouseInputStream) {
            return (ClickHouseInputStream) input;
        }
        return wrap(null, input, bufferSize, compression, compressionLevel, postCloseAction);
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
        return of(Arrays.asList(files), File.class, null, ClickHouseDataConfig.getDefaultReadBufferSize(), null);
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
            return of(inputs[0]);
        }
        return of(Arrays.asList(inputs), InputStream.class, null, ClickHouseDataConfig.getDefaultReadBufferSize(),
                null);
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
        return of(Arrays.asList(strings), String.class, null, ClickHouseDataConfig.getDefaultReadBufferSize(), null);
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
        return of(Arrays.asList(urls), URL.class, null, ClickHouseDataConfig.getDefaultReadBufferSize(), null);
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
        return of(Arrays.asList(source), clazz, converter, ClickHouseDataConfig.getDefaultReadBufferSize(),
                postCloseAction);
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
    public static <T> ClickHouseInputStream of(Iterable<T> source, Class<T> clazz, Function<T, byte[]> converter,
            Runnable postCloseAction) {
        return of(source, clazz, converter, ClickHouseDataConfig.getDefaultReadBufferSize(), postCloseAction);
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
     * @param bufferSize      read buffer size
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return non-null input stream
     */
    @SuppressWarnings("unchecked")
    public static <T> ClickHouseInputStream of(Iterable<T> source, Class<T> clazz, Function<T, byte[]> converter,
            int bufferSize, Runnable postCloseAction) {
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
            }, bufferSize, postCloseAction);
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
            }, bufferSize, postCloseAction);
        } else if (InputStream.class.isAssignableFrom(clazz)) {
            return new IterableMultipleInputStream<>((Iterable<InputStream>) source, i -> i, bufferSize,
                    postCloseAction);
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
     *                   {@link ClickHouseDataConfig#DEFAULT_BUFFER_SIZE}
     * @return written bytes
     * @throws IOException when error occured reading from input stream or writing
     *                     data to output stream
     */
    public static long pipe(InputStream input, OutputStream output, int bufferSize) throws IOException {
        if (input instanceof ClickHouseInputStream && output instanceof ClickHouseOutputStream) {
            return ((ClickHouseInputStream) input).pipe((ClickHouseOutputStream) output);
        }

        return pipe(input, output, new byte[ClickHouseDataConfig.getBufferSize(bufferSize)]);
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

    /**
     * Saves data from the given input stream to a temporary file, which will be
     * deleted after JVM exited.
     *
     * @param input non-null input stream
     * @return non-null temporary file
     */
    public static File save(InputStream input) {
        return save(null, input, null);
    }

    /**
     * Saves data from the given input stream to the specified file.
     *
     * @param input non-null input stream
     * @param file  target file, could be null
     * @return non-null file
     */
    public static File save(InputStream input, File file) {
        return save(null, input, file);
    }

    /**
     * Saves data from the given input stream to a temporary file, which will be
     * deleted after JVM exited.
     *
     * @param config config, could be null
     * @param input  non-null input stream
     * @return non-null temporary file
     */
    public static File save(ClickHouseDataConfig config, InputStream input) {
        return save(config, input, null);
    }

    /**
     * Saves data from the given input stream to the specified file.
     *
     * @param config config, could be null
     * @param input  non-null input stream
     * @param file   target file, could be null
     * @return non-null file
     */
    public static File save(ClickHouseDataConfig config, InputStream input, File file) {
        final File tmp;
        if (file != null) {
            tmp = file;
        } else {
            try {
                tmp = ClickHouseUtils.createTempFile();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create temp file", e);
            }
        }

        final int bufferSize;
        final long timeout;
        if (config != null) {
            bufferSize = config.getWriteBufferSize();
            timeout = config.getWriteTimeout();
        } else {
            bufferSize = ClickHouseDataConfig.DEFAULT_WRITE_BUFFER_SIZE;
            timeout = ClickHouseDataConfig.DEFAULT_TIMEOUT;
        }

        if (timeout <= 0L) {
            try {
                try (OutputStream out = new FileOutputStream(tmp)) {
                    pipe(input, out, bufferSize);
                }
                return tmp;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            CompletableFuture<File> data = ClickHouseDataStreamFactory.getInstance().runBlockingTask(() -> {
                try {
                    try (OutputStream out = new FileOutputStream(tmp)) {
                        pipe(input, out, bufferSize);
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
    }

    /**
     * Saves data from the given input stream to a temporary file.
     *
     * @param in         non-null input stream
     * @param bufferSize buffer size
     * @param timeout    timeout in milliseconds
     * @return non-null temporary file
     * @deprecated will be dropped in 0.5, please use {@link #save(InputStream)}
     *             instead
     */
    @Deprecated
    public static File save(InputStream in, int bufferSize, int timeout) {
        return save(null, in, bufferSize, timeout, true);
    }

    /**
     * Saves data from the given input stream to the specified file.
     *
     * @param file         target file, could be null
     * @param in           non-null input stream
     * @param bufferSize   buffer size
     * @param timeout      timeout in milliseconds
     * @param deleteOnExit whether the file should be deleted after JVM exit
     * @return non-null file
     * @deprecated will be dropped in 0.5, please use
     *             {@link #save(InputStream, File)} instead
     */
    @Deprecated
    public static File save(File file, InputStream in, int bufferSize, int timeout, boolean deleteOnExit) {
        final File tmp;
        if (file != null) {
            tmp = file;
            if (deleteOnExit) {
                tmp.deleteOnExit();
            }
        } else {
            try {
                tmp = ClickHouseUtils.createTempFile("chc", "data", true);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create temp file", e);
            }
        }
        CompletableFuture<File> data = ClickHouseDataStreamFactory.getInstance().runBlockingTask(() -> {
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
     * Underlying pass-thru stream.
     */
    protected final ClickHousePassThruStream stream;
    /**
     * Optional post close action.
     */
    protected final Runnable postCloseAction;
    /**
     * User data shared between multiple calls.
     */
    protected final Map<String, Object> userData;

    protected volatile boolean closed;

    protected OutputStream copyTo;

    protected ClickHouseInputStream(ClickHousePassThruStream stream, OutputStream copyTo, Runnable postCloseAction) {
        this.byteBuffer = ClickHouseByteBuffer.newInstance();
        this.stream = stream != null ? stream : ClickHousePassThruStream.NULL;
        this.postCloseAction = postCloseAction;
        this.userData = new HashMap<>();
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
     * Gets reference to current byte buffer.
     *
     * @return non-null byte buffer
     */
    protected abstract ClickHouseByteBuffer getBuffer();

    /**
     * Checks whether the internal buffer is reused among multiple reads.
     *
     * @return true if the internal buffer is reused among multiple reads; false
     *         otherwise
     */
    protected boolean reusableBuffer() {
        return false;
    }

    /**
     * Gets reference to next byte buffer available for read. An empty byte buffer
     * will be returned ({@code nextBuffer().isEmpty() == true}), when it reaches
     * end of the input stream.
     *
     * @return non-null byte buffer
     */
    protected abstract ClickHouseByteBuffer nextBuffer() throws IOException;

    /**
     * Gets underlying file. Same as
     * {@code ClickHouseFile.of(getUnderlyingStream())}.
     *
     * @return non-null underlying file
     */
    public ClickHouseFile getUnderlyingFile() {
        return ClickHouseFile.of(stream);
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
     * Checks if there's underlying input stream. Same as
     * {@code getUnderlyingStream().hasInput()}.
     *
     * @return true if there's underlying input stream; false otherwise
     */
    public boolean hasUnderlyingStream() {
        return stream.hasInput();
    }

    /**
     * Gets user data associated with this input stream.
     *
     * @param key key
     * @return value, could be null
     */
    public final Object getUserData(String key) {
        return userData.get(key);
    }

    /**
     * Gets user data associated with this input stream.
     *
     * @param <T>          type of the value
     * @param key          key
     * @param defaultValue default value
     * @return value, could be null
     */
    @SuppressWarnings("unchecked")
    public final <T> T getUserData(String key, T defaultValue) {
        return (T) userData.getOrDefault(key, defaultValue);
    }

    /**
     * Removes user data.
     *
     * @param key key
     * @return removed user data, could be null
     */
    public final Object removeUserData(String key) {
        return userData.remove(key);
    }

    /**
     * Sets user data.
     *
     * @param key   key
     * @param value value
     * @return overidded value, could be null
     */
    @SuppressWarnings("unchecked")
    public final <T> T setUserData(String key, T value) {
        return (T) userData.put(key, value);
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
    public long pipe(ClickHouseOutputStream output) throws IOException {
        long count = 0L;
        if (output == null || output.isClosed()) {
            return count;
        }
        ensureOpen();

        try (ClickHouseInputStream in = this) {
            for (ClickHouseByteBuffer buf : in) {
                count += buf.length();
                output.writeBuffer(buf);
            }
        }
        return count;
    }

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
     * Reads byte buffer from the input stream until the first match of the
     * separator.
     *
     * @param separator non-empty separator
     * @return non-null byte buffer
     * @throws IOException when failed to read bytes from input stream or not able
     *                     to retrieve all bytes
     */
    public abstract ClickHouseByteBuffer readBufferUntil(byte[] separator) throws IOException;

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
                        : new StreamCorruptedException(ClickHouseUtils.format(ERROR_INCOMPLETE_READ, offset, length));
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
            ClickHouseByteBuffer buf = getBuffer();
            if (!buf.isEmpty()) {
                out.write(buf.array(), buf.position(), buf.length());
            }
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
            // clear user data if any
            userData.clear();
            // don't want to hold the last byte array reference for too long
            byteBuffer.reset();
            ClickHouseDataStreamFactory.handleCustomAction(postCloseAction);
        }
    }

    @Override
    public Iterator<ClickHouseByteBuffer> iterator() {
        return new ByteBufferIterator(this);
    }
}
