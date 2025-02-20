package com.clickhouse.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;

/**
 * Wrapper of {@link java.io.File} with additional information like compression
 * and format.
 */
@Deprecated
public class ClickHouseFile extends ClickHousePassThruStream {
    private static final long serialVersionUID = -2641191818870839568L;

    private static final String FILE_TYPE_NAME = "File";

    /**
     * Null file which has no compression and format.
     */
    public static final ClickHouseFile NULL = new ClickHouseFile(null, ClickHouseCompression.NONE,
            ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, null);

    public static ClickHouseFile of(File file) {
        return of(file, null, ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL, null);
    }

    public static ClickHouseFile of(Path path) {
        return of(ClickHouseChecker.nonNull(path, "Path").toFile(), null,
                ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL, null);
    }

    public static ClickHouseFile of(String file) {
        return of(new File(ClickHouseChecker.nonEmpty(file, FILE_TYPE_NAME)), null,
                ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL, null);
    }

    public static ClickHouseFile of(String file, ClickHouseCompression compression, ClickHouseFormat format) {
        return of(file, compression, ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL, format);
    }

    public static ClickHouseFile of(String file, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        return of(new File(ClickHouseChecker.nonEmpty(file, FILE_TYPE_NAME)), compression, compressionLevel, format);
    }

    public static ClickHouseFile of(File file, ClickHouseCompression compression, ClickHouseFormat format) {
        return of(file, compression, ClickHouseDataConfig.DEFAULT_COMPRESS_LEVEL, format);
    }

    public static ClickHouseFile of(File file, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        final String name = ClickHouseChecker.nonNull(file, FILE_TYPE_NAME).getName();
        return new ClickHouseFile(file,
                compression != null ? compression : ClickHouseCompression.fromFileName(name),
                compressionLevel, format != null ? format : ClickHouseFormat.fromFileName(name));
    }

    public static ClickHouseFile of(ClickHousePassThruStream stream) {
        if (stream instanceof ClickHouseFile) {
            return (ClickHouseFile) stream;
        } else if (!stream.hasInput()) {
            throw new IllegalArgumentException(ClickHousePassThruStream.ERROR_NO_INPUT);
        }

        return of(stream.getInputStream(), stream.getCompressionAlgorithm(), stream.getCompressionLevel(),
                stream.getFormat());
    }

    public static ClickHouseFile of(ClickHouseInputStream input, ClickHouseCompression compression,
            int compressionLevel, ClickHouseFormat format) {
        final ClickHousePassThruStream stream = ClickHouseChecker.nonNull(input, ClickHouseInputStream.TYPE_NAME)
                .getUnderlyingStream();
        if (stream instanceof ClickHouseFile) {
            return ((ClickHouseFile) stream);
        }

        final boolean hasInput = stream.hasInput();
        final File tmp;
        try {
            tmp = ClickHouseUtils.createTempFile();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to create temporary file", e);
        }

        try (ClickHouseOutputStream out = ClickHouseOutputStream.of(new FileOutputStream(tmp))) {
            (hasInput ? stream.getInputStream() : input).pipe(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return hasInput ? of(tmp, stream.getCompressionAlgorithm(), stream.getCompressionLevel(), stream.getFormat())
                : of(tmp, compression, compressionLevel, format);
    }

    public static ClickHouseFile of(InputStream input, ClickHouseCompression compression, int compressionLevel,
            ClickHouseFormat format) {
        if (ClickHouseChecker.nonNull(input, ClickHouseInputStream.TYPE_NAME) instanceof ClickHouseInputStream) {
            return of((ClickHouseInputStream) input, compression, compressionLevel, format);
        }

        final File tmp;
        try {
            tmp = ClickHouseUtils.createTempFile();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to create temporary file", e);
        }

        try (ClickHouseOutputStream out = ClickHouseOutputStream.of(new FileOutputStream(tmp))) {
            ClickHouseInputStream.of(input).pipe(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return of(tmp, compression, compressionLevel, format);
    }

    private final File file;

    protected ClickHouseFile(File file, ClickHouseCompression compress, int compressLevel, ClickHouseFormat format) {
        super(null, null, compress, compressLevel, format);

        this.file = file;
    }

    @Override
    public ClickHouseInputStream getInputStream() {
        return ClickHouseInputStream.of(getFile());
    }

    @Override
    public ClickHouseInputStream newInputStream(int bufferSize, Runnable postCloseAction) {
        if (!hasInput()) {
            return ClickHouseInputStream.empty();
        }

        try {
            return ClickHouseInputStream.wrap(this, new FileInputStream(getFile()), bufferSize,
                    ClickHouseCompression.NONE, getCompressionLevel(), postCloseAction);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClickHouseOutputStream getOutputStream() {
        try {
            return ClickHouseOutputStream.of(new FileOutputStream(getFile()));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ClickHouseOutputStream newOutputStream(int bufferSize, Runnable postCloseAction) {
        if (!hasOutput()) {
            return ClickHouseOutputStream.empty();
        }

        try {
            return ClickHouseOutputStream.wrap(this, new FileOutputStream(getFile()),
                    bufferSize, ClickHouseCompression.NONE, getCompressionLevel(), postCloseAction);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Gets file. Use {@code #hasInput()} or {@code #hasOutput()} to check file
     * availability first.
     *
     * @return file, could be null
     */
    public File getFile() {
        return file;
    }

    /**
     * Checks if the given file is recogonized or not. Same as
     * {@code hasCompression() || hasFormat()}.
     *
     * @return true if the file is recogonized
     */
    public boolean isRecognized() {
        return hasCompression() || hasFormat();
    }

    @Override
    public boolean hasInput() {
        return file != null && file.exists();
    }

    @Override
    public boolean hasOutput() {
        return file != null;
    }
}
