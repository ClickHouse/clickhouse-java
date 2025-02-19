package com.clickhouse.data;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * This class encapsulates custom input and output stream to ensure no
 * compression/decompression will be applied during execution.
 */
@Deprecated
public class ClickHousePassThruStream implements Serializable {
    private static final long serialVersionUID = -879012829388929569L;

    public static final String TYPE_NAME = "Pass-thru Stream";

    public static final String ERROR_NO_INPUT = "Pass-thru stream does not have input";
    public static final String ERROR_NO_OUTPUT = "Pass-thru stream does not have output";

    /**
     * Null stream which has no compression and format.
     */
    public static final ClickHousePassThruStream NULL = new ClickHousePassThruStream(null, null,
            ClickHouseCompression.NONE, ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL, null);

    public static ClickHousePassThruStream of(InputStream in, ClickHouseCompression compression,
            ClickHouseFormat format) {
        return of(in, null, compression, ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, format);
    }

    public static ClickHousePassThruStream of(InputStream in, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        return of(in, null, compression, compressionLevel, format);
    }

    public static ClickHousePassThruStream of(ClickHouseWriter writer, ClickHouseCompression compression,
            ClickHouseFormat format) {
        return of(ClickHouseInputStream.of(writer), null, compression,
                ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, format);
    }

    public static ClickHousePassThruStream of(ClickHouseWriter writer, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        return of(ClickHouseInputStream.of(writer), null, compression, compressionLevel, format);
    }

    public static ClickHousePassThruStream of(OutputStream out, ClickHouseCompression compression,
            ClickHouseFormat format) {
        return of(null, out, compression, ClickHouseDataConfig.DEFAULT_WRITE_COMPRESS_LEVEL, format);
    }

    public static ClickHousePassThruStream of(OutputStream out, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        return of(null, out, compression, compressionLevel, format);
    }

    public static ClickHousePassThruStream of(InputStream in, OutputStream out, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        if (in == null && out == null && compression == null
                && compressionLevel == ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL && format == null) {
            return NULL;
        }

        return new ClickHousePassThruStream(in, out, compression, compressionLevel, format);
    }

    private final transient InputStream input;
    private final transient OutputStream output;
    private final ClickHouseCompression compression;
    private final int compressionLevel;
    private final ClickHouseFormat format;

    protected ClickHousePassThruStream(InputStream in, OutputStream out, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        this.input = in;
        this.output = out;
        this.compression = compression;
        this.compressionLevel = compressionLevel;
        this.format = format;
    }

    /**
     * Gets the input stream for reading. Please pay attention that the returned
     * input stream has nothing to do with this pass-thru stream, as
     * {@code getInputStream().getUnderlyingStream()} is always {@link #NULL}.
     *
     * @return non-null input stream
     */
    public ClickHouseInputStream getInputStream() {
        return input != null ? ClickHouseInputStream.of(input) : ClickHouseInputStream.empty();
    }

    /**
     * Creates a wrapped input stream for reading. Calling this method multiple
     * times will generate multiple {@link ClickHouseInputStream} instances pointing
     * to the same input stream, so it does not make sense to call this more than
     * once. Unlike {@link #getInputStream()}, the returned input stream is
     * associated with this pass-thru stream, so
     * {@code newInputStream(...).getUnderlyingStream()} simply returns the current
     * pass-thru stream.
     *
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return non-null wrapped input stream
     */
    public ClickHouseInputStream newInputStream(int bufferSize, Runnable postCloseAction) {
        return input != null
                ? ClickHouseInputStream.wrap(this, input, bufferSize, ClickHouseCompression.NONE, compressionLevel,
                        postCloseAction)
                : ClickHouseInputStream.empty();
    }

    /**
     * Gets the output stream for writing. Please pay attention that the returned
     * output stream has nothing to do with this pass-thru stream, as
     * {@code getOutputStream().getUnderlyingStream()} is always {@link #NULL}.
     *
     * @return non-null output stream
     */
    public ClickHouseOutputStream getOutputStream() {
        return output != null ? ClickHouseOutputStream.of(output) : ClickHouseOutputStream.empty();
    }

    /**
     * Creates a wrapped output stream for writing. Calling this method multiple
     * times will generate multiple {@link ClickHouseOutputStream} instances, so it
     * does not make sense to call this more than once. Unlike
     * {@link #getOutputStream()}, the returned
     * output stream is associated with this pass-thru stream, so
     * {@code newOutputStream(...).getUnderlyingStream()} simply returns the current
     * pass-thru stream.
     *
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        4096 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return non-null wrapped output stream
     */
    public ClickHouseOutputStream newOutputStream(int bufferSize, Runnable postCloseAction) {
        return output != null
                ? ClickHouseOutputStream.wrap(this, output, bufferSize, ClickHouseCompression.NONE,
                        compressionLevel, postCloseAction)
                : ClickHouseOutputStream.empty();
    }

    /**
     * Gets compression algorithm, which could be null. Use
     * {@link #hasCompression()} to check first.
     *
     * @return compression algorithm, could be null
     */
    public ClickHouseCompression getCompressionAlgorithm() {
        return compression;
    }

    /**
     * Gets compression level.
     *
     * @return compression level
     */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    /**
     * Gets data format, which could be null. Use {@link #hasFormat()} to check
     * first.
     *
     * @return data format, could be null
     */
    public ClickHouseFormat getFormat() {
        return format;
    }

    /**
     * Checks if the compression algorithm is defined or not.
     *
     * @return true if the compression algorithm is defined; false otherwise
     */
    public boolean hasCompression() {
        return compression != null;
    }

    /**
     * Checks if the data format is defined or not.
     *
     * @return true if the data format is defined; false otherwise
     */
    public boolean hasFormat() {
        return format != null;
    }

    /**
     * Checks if input stream is available or not.
     *
     * @return true if input stream is available; false otherwise
     */
    public boolean hasInput() {
        return input != null;
    }

    /**
     * Checks if output stream is available or not.
     *
     * @return true if output stream is available; false otherwise
     */
    public boolean hasOutput() {
        return output != null;
    }

    /**
     * Checks if the data is compressed or not.
     *
     * @return true if the data is compressed; false otherwise
     */
    public boolean isCompressed() {
        return hasCompression() && compression != ClickHouseCompression.NONE;
    }
}
