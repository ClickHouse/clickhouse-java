package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseUtils;

public final class CompressionUtils {
    public static final String ERROR_FAILED_TO_WRAP_INPUT = "Failed to wrap input stream";
    public static final String ERROR_FAILED_TO_WRAP_OUTPUT = "Failed to wrap output stream";
    public static final String ERROR_UNSUPPORTED_COMPRESS_ALG = "Compression algorithm [%s] is not supported due to %s";
    public static final String ERROR_UNSUPPORTED_DECOMPRESS_ALG = "Decompression algorithm [%s] is not supported due to %s";

    private static class Brotli4jUtils {
        static {
            com.aayushatharva.brotli4j.Brotli4jLoader.ensureAvailability();
        }

        static InputStream createInputStream(InputStream input, int bufferSize) throws IOException {
            return new com.aayushatharva.brotli4j.decoder.BrotliInputStream(input, bufferSize);
        }

        static OutputStream createOutputStream(OutputStream output, int quality, int bufferSize)
                throws IOException {
            com.aayushatharva.brotli4j.encoder.Encoder.Parameters params = new com.aayushatharva.brotli4j.encoder.Encoder.Parameters()
                    .setQuality(quality);
            return new com.aayushatharva.brotli4j.encoder.BrotliOutputStream(output, params, bufferSize);
        }

        private Brotli4jUtils() {
        }
    }

    public static InputStream createBrotliInputStream(InputStream input, int bufferSize) {
        try {
            // Brotli4jUtils.createInputStream(input, bufferSize)
            return new org.brotli.dec.BrotliInputStream(input, bufferSize);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_INPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, ClickHouseCompression.BROTLI,
                            e.getMessage()));
        }
    }

    public static OutputStream createBrotliOutputStream(OutputStream output, int quality, int bufferSize) {
        try {
            if (quality < -1 || quality > 11) {
                quality = 4;
            }
            return Brotli4jUtils.createOutputStream(output, quality, bufferSize);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_OUTPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, ClickHouseCompression.BROTLI,
                            e.getMessage()));
        }
    }

    public static InputStream createBz2InputStream(InputStream input) {
        try {
            return new org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream(input);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_INPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, ClickHouseCompression.BZ2,
                            e.getMessage()));
        }
    }

    public static OutputStream createBz2OutputStream(OutputStream output, int blockSize) {
        try {
            if (blockSize < org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream.MIN_BLOCKSIZE
                    || blockSize > org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream.MAX_BLOCKSIZE) {
                blockSize = org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream.MAX_BLOCKSIZE;
            }
            return new org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream(output, blockSize);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_OUTPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, ClickHouseCompression.BROTLI,
                            e.getMessage()));
        }
    }

    public static InputStream createSnappyInputStream(InputStream input) {
        try {
            return new org.xerial.snappy.SnappyInputStream(input);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_INPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, ClickHouseCompression.SNAPPY,
                            e.getMessage()));
        }
    }

    public static OutputStream createSnappyOutputStream(OutputStream output, int blockSize) {
        try {
            if (blockSize < 1024) { // MIN_BLOCK_SIZE
                blockSize = 32 * 1024; // DEFAULT_BLOCK_SIZE
            }
            return new org.xerial.snappy.SnappyOutputStream(output, blockSize);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, ClickHouseCompression.SNAPPY,
                            e.getMessage()));
        }
    }

    public static InputStream createZstdInputStream(InputStream input) {
        try {
            return new com.github.luben.zstd.ZstdInputStream(input).setContinuous(true);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_INPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, ClickHouseCompression.ZSTD,
                            e.getMessage()));
        }
    }

    /**
     * Creates an output stream for {@link ClickHouseCompression#ZSTD} compression.
     *
     * @param output non-null output stream
     * @param level  compression level, any number outside of [0, 22] will be
     *               treated as default
     * @return output stream for compression
     */
    public static OutputStream createZstdOutputStream(OutputStream output, int level) {
        try {
            if (level < 0 || level > 22) {
                level = com.github.luben.zstd.Zstd.defaultCompressionLevel();
            }
            return new com.github.luben.zstd.ZstdOutputStream(output, level);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_OUTPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, ClickHouseCompression.ZSTD, e.getMessage()));
        }
    }

    public static InputStream createXzInputStream(InputStream input) {
        try {
            return new org.tukaani.xz.XZInputStream(input);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_INPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, ClickHouseCompression.XZ, e.getMessage()));
        }
    }

    /**
     * Creates an output stream for {@link ClickHouseCompression#XZ} compression.
     *
     * @param output non-null output stream
     * @param preset compression preset level, any number outside of [0,9] will be
     *               treated as 6, which is the default
     * @return output stream for compression
     */
    public static OutputStream createXzOutputStream(OutputStream output, int preset) {
        try {
            if (preset < org.tukaani.xz.LZMA2Options.PRESET_MIN || preset > org.tukaani.xz.LZMA2Options.PRESET_MAX) {
                preset = org.tukaani.xz.LZMA2Options.PRESET_DEFAULT;
            }
            return new org.tukaani.xz.XZOutputStream(output, new org.tukaani.xz.LZMA2Options(preset),
                    org.tukaani.xz.XZ.CHECK_CRC64);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_OUTPUT, e);
        } catch (NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, ClickHouseCompression.XZ, e.getMessage()));
        }
    }

    private CompressionUtils() {
    }
}
