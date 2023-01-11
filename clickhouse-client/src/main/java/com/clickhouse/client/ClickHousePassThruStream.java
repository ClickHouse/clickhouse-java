package com.clickhouse.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * This class encapsulates custom input and output stream to ensure no
 * compression/decompression will be applied during execution.
 */
public class ClickHousePassThruStream implements Serializable {
    private static final long serialVersionUID = -879012829388929569L;

    /**
     * Null stream which has no compression and format.
     */
    public static final ClickHousePassThruStream NULL = new ClickHousePassThruStream(null, null,
            ClickHouseCompression.NONE, -1, null);

    public static ClickHousePassThruStream of(InputStream in, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        return of(in, null, compression, compressionLevel, format);
    }

    public static ClickHousePassThruStream of(OutputStream out, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        return of(null, out, compression, compressionLevel, format);
    }

    public static ClickHousePassThruStream of(InputStream in, OutputStream out, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        if (in == null && out == null && (compression == null || compression == ClickHouseCompression.NONE)
                && compressionLevel == -1 && format == null) {
            return NULL;
        }

        return new ClickHousePassThruStream(in, out, compression == null ? ClickHouseCompression.NONE : compression,
                compressionLevel, format);
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
     * Creates an input stream for reading the pass-thru input stream.
     *
     * @return non-null input stream for reading from the pass-thru input stream
     */
    public ClickHouseInputStream asInputStream() {
        return input != null ? ClickHouseInputStream.of(input) : ClickHouseInputStream.empty();
    }

    /**
     * Creates an input stream for reading the pass-thru input stream.
     *
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        8192 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return non-null input stream for reading from the pass-thru input stream
     */
    public ClickHouseInputStream asInputStream(int bufferSize, Runnable postCloseAction) {
        return input != null
                ? ClickHouseInputStream.wrap(null, input, bufferSize, postCloseAction, ClickHouseCompression.NONE,
                        compressionLevel)
                : ClickHouseInputStream.empty();
    }

    /**
     * Creates an output stream for writing data into the pass-thru output stream.
     *
     * @return non-null output stream for writing data into the the pass-thru output
     *         stream
     */
    public ClickHouseOutputStream asOutputStream() {
        return output != null ? ClickHouseOutputStream.of(output) : ClickHouseOutputStream.empty();
    }

    /**
     * Creates an output stream for writing data into the pass-thru output stream.
     *
     * @param bufferSize      buffer size which is always greater than zero(usually
     *                        8192 or larger)
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return non-null output stream for writing data into the the pass-thru output
     *         stream
     */
    public ClickHouseOutputStream asOutputStream(int bufferSize, Runnable postCloseAction) {
        return output != null
                ? ClickHouseOutputStream.wrap(null, output, bufferSize, postCloseAction, ClickHouseCompression.NONE,
                        compressionLevel)
                : ClickHouseOutputStream.empty();
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
     * Gets compression algorithm.
     *
     * @return non-null compression algorithm
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
     * Checks if either input or output stream is available or not.
     *
     * @return true if either input or output stream is available; false otherwise
     */
    public boolean isAvailable() {
        return input != null || output != null;
    }

    /**
     * Checks if the data is compressed or not.
     *
     * @return true if the data is compressed; false otherwise
     */
    public boolean isCompressed() {
        return compression != ClickHouseCompression.NONE;
    }
}
