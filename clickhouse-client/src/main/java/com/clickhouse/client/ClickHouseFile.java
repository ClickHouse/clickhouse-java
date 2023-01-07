package com.clickhouse.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.nio.file.Path;

import com.clickhouse.client.config.ClickHouseClientOption;

/**
 * Wrapper of {@link java.io.File} with additional information like compression
 * and format.
 */
public class ClickHouseFile implements Serializable {
    private static final long serialVersionUID = -2641191818870839568L;

    /**
     * Null file which has no compression and format.
     */
    public static final ClickHouseFile NULL = new ClickHouseFile(null, ClickHouseCompression.NONE, 0, null);

    public static ClickHouseFile of(File file) {
        return of(file, null, 0, null);
    }

    public static ClickHouseFile of(Path path) {
        return of(ClickHouseChecker.nonNull(path, "Path").toFile(), null, 0, null);
    }

    public static ClickHouseFile of(String file) {
        return of(new File(ClickHouseChecker.nonEmpty(file, "File")), null, 0, null);
    }

    public static ClickHouseFile of(String file, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        return of(new File(ClickHouseChecker.nonEmpty(file, "File")), compression, compressionLevel, format);
    }

    public static ClickHouseFile of(File file, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        return new ClickHouseFile(ClickHouseChecker.nonNull(file, "File"),
                compression != null ? compression : ClickHouseCompression.fromFileName(file.getName()),
                compressionLevel < 1 ? 0 : compressionLevel,
                format != null ? format : ClickHouseFormat.fromFileName(file.getName()));
    }

    private final File file;
    private final ClickHouseCompression compress;
    private final int compressLevel;
    private final ClickHouseFormat format;

    protected ClickHouseFile(File file, ClickHouseCompression compress, int compressLevel, ClickHouseFormat format) {
        this.file = file;
        this.compress = compress;
        this.compressLevel = compressLevel;
        this.format = format;
    }

    /**
     * Creates an input stream for reading the file.
     *
     * @return non-null input stream for reading the file
     */
    public ClickHouseInputStream asInputStream() {
        if (!isAvailable()) {
            return ClickHouseInputStream.empty();
        }

        try {
            return ClickHouseInputStream.wrap(this, new FileInputStream(getFile()),
                    (int) ClickHouseClientOption.READ_BUFFER_SIZE.getDefaultValue(), null,
                    ClickHouseCompression.NONE, getCompressionLevel());
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Creates an output stream for writing data into the file.
     *
     * @return non-null input stream for writing data into the file
     */
    public ClickHouseOutputStream asOutputStream() {
        if (!isAvailable()) {
            return ClickHouseOutputStream.empty();
        }

        try {
            return ClickHouseOutputStream.wrap(this, new FileOutputStream(getFile()),
                    (int) ClickHouseClientOption.WRITE_BUFFER_SIZE.getDefaultValue(), null,
                    ClickHouseCompression.NONE, getCompressionLevel());
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Gets file, which only works when {@link #isAvailable()} returns {@code true}.
     *
     * @return non-null file, except {@code null} for {@link #NULL}
     */
    public File getFile() {
        return file;
    }

    /**
     * Gets file format, which could be null. Use {@link #hasFormat()} to check
     * first.
     *
     * @return file format, could be null
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
        return compress;
    }

    /**
     * Gets compression level.
     *
     * @return compression level, which in general should be greater than or equal
     *         to zero
     */
    public int getCompressionLevel() {
        return compressLevel;
    }

    /**
     * Checks if the file format is defined or not.
     *
     * @return true if the file format is defined; false otherwise
     */
    public boolean hasFormat() {
        return format != null;
    }

    /**
     * Checks if the file is available or not.
     *
     * @return true if the file is available; false otherwise
     */
    public boolean isAvailable() {
        return file != null && file.exists();
    }

    /**
     * Checks if the file is compressed or not.
     *
     * @return true if the file is compressed; false otherwise
     */
    public boolean isCompressed() {
        return compress != ClickHouseCompression.NONE;
    }
}
