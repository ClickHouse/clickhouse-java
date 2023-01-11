package com.clickhouse.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.file.Path;

import com.clickhouse.client.config.ClickHouseClientOption;

/**
 * Wrapper of {@link java.io.File} with additional information like compression
 * and format.
 */
public class ClickHouseFile extends ClickHousePassThruStream {
    private static final long serialVersionUID = -2641191818870839568L;

    /**
     * Null file which has no compression and format.
     */
    public static final ClickHouseFile NULL = new ClickHouseFile(null, ClickHouseCompression.NONE, -1, null);

    public static ClickHouseFile of(File file) {
        return of(file, null, 0, null);
    }

    public static ClickHouseFile of(Path path) {
        return of(ClickHouseChecker.nonNull(path, "Path").toFile(), null, -1, null);
    }

    public static ClickHouseFile of(String file) {
        return of(new File(ClickHouseChecker.nonEmpty(file, "File")), null, -1, null);
    }

    public static ClickHouseFile of(String file, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        return of(new File(ClickHouseChecker.nonEmpty(file, "File")), compression, compressionLevel, format);
    }

    public static ClickHouseFile of(File file, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        return new ClickHouseFile(ClickHouseChecker.nonNull(file, "File"),
                compression != null ? compression : ClickHouseCompression.fromFileName(file.getName()),
                compressionLevel, format != null ? format : ClickHouseFormat.fromFileName(file.getName()));
    }

    private final File file;

    protected ClickHouseFile(File file, ClickHouseCompression compress, int compressLevel, ClickHouseFormat format) {
        super(null, null, compress, compressLevel, format);

        this.file = file;
    }

    @Override
    public ClickHouseInputStream asInputStream() {
        return asInputStream((int) ClickHouseClientOption.READ_BUFFER_SIZE.getDefaultValue(), null);
    }

    @Override
    public ClickHouseInputStream asInputStream(int bufferSize, Runnable postCloseAction) {
        if (!isAvailable()) {
            return ClickHouseInputStream.empty();
        }

        try {
            return ClickHouseInputStream.wrap(this, new FileInputStream(getFile()), bufferSize, postCloseAction,
                    ClickHouseCompression.NONE, getCompressionLevel());
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClickHouseOutputStream asOutputStream() {
        return asOutputStream((int) ClickHouseClientOption.WRITE_BUFFER_SIZE.getDefaultValue(), null);
    }

    @Override
    public ClickHouseOutputStream asOutputStream(int bufferSize, Runnable postCloseAction) {
        if (!isAvailable()) {
            return ClickHouseOutputStream.empty();
        }

        try {
            return ClickHouseOutputStream.wrap(this, new FileOutputStream(getFile()),
                    bufferSize, postCloseAction, ClickHouseCompression.NONE, getCompressionLevel());
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

    @Override
    public boolean hasInput() {
        return isAvailable();
    }

    @Override
    public boolean hasOutput() {
        return isAvailable();
    }

    @Override
    public boolean isAvailable() {
        return file != null && file.exists();
    }
}
