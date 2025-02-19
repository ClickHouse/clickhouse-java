package com.clickhouse.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.clickhouse.config.ClickHouseDefaultOption;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.compress.BrotliSupport;
import com.clickhouse.data.compress.Bz2Support;
import com.clickhouse.data.compress.DeflateSupport;
import com.clickhouse.data.compress.GzipSupport;
import com.clickhouse.data.compress.Lz4Support;
import com.clickhouse.data.compress.NoneSupport;
import com.clickhouse.data.compress.SnappySupport;
import com.clickhouse.data.compress.XzSupport;
import com.clickhouse.data.compress.ZstdSupport;

@Deprecated
public interface ClickHouseCompressionAlgorithm {
    static final String ERROR_FAILED_TO_WRAP_INPUT = "Failed to wrap input stream";
    static final String ERROR_FAILED_TO_WRAP_OUTPUT = "Failed to wrap output stream";

    static final String ERROR_UNSUPPORTED_COMPRESS_ALG = "%s is not supported. Please disable decompression(decompress=0), modify the algorithm(e.g. decompress_algorithm=gzip), or add the missing libraries to the classpath.";
    static final String ERROR_UNSUPPORTED_DECOMPRESS_ALG = "%s is not supported. Please disable compression(compress=0), modify the algorithm(e.g. compress_algorithm=gzip), or add the missing libraries to the classpath.";

    static final ClickHouseOption COMPRESSION_LIB_DETECTION = new ClickHouseDefaultOption("compression_lib_detection",
            true);

    static ClickHouseCompressionAlgorithm createInstance(String option,
            Class<? extends ClickHouseCompressionAlgorithm> preferredClass,
            Class<? extends ClickHouseCompressionAlgorithm> defaultClass) {
        ClickHouseCompressionAlgorithm alg = null;
        if ((boolean) new ClickHouseDefaultOption(option,
                (boolean) ClickHouseCompressionAlgorithm.COMPRESSION_LIB_DETECTION.getEffectiveDefaultValue())
                .getEffectiveDefaultValue()) {
            try {
                alg = preferredClass.getDeclaredConstructor().newInstance();
            } catch (Throwable t) { // NOSONAR
                // ignore
            }
        }

        if (alg == null) {
            try {
                alg = defaultClass.getDeclaredConstructor().newInstance();
            } catch (Throwable e) { // NOSONAR
                throw new UnsupportedOperationException("Failed to create default instance of " + defaultClass, e);
            }
        }
        return alg;
    }

    static ClickHouseInputStream createInputStream(ClickHousePassThruStream stream, InputStream input, int bufferSize,
            ClickHouseCompression compression, int level, Runnable postCloseAction) {
        try {
            return of(compression).decompress(stream, input, bufferSize, level, postCloseAction);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_INPUT, e);
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, compression));
        }
    }

    static ClickHouseOutputStream createOutputStream(ClickHousePassThruStream stream, OutputStream output,
            int bufferSize, ClickHouseCompression compression, int level, Runnable postCloseAction) {
        try {
            return of(compression).compress(stream, output, bufferSize, level, postCloseAction);
        } catch (IOException e) {
            throw new IllegalArgumentException(ERROR_FAILED_TO_WRAP_OUTPUT, e);
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            throw new UnsupportedOperationException(
                    ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, compression));
        }
    }

    static ClickHouseCompressionAlgorithm of(ClickHouseCompression compression) {
        if (compression == null || compression == ClickHouseCompression.NONE) {
            return NoneSupport.getInstance();
        }

        final ClickHouseCompressionAlgorithm alg;
        switch (compression) {
            case BROTLI:
                alg = BrotliSupport.getInstance();
                break;
            case BZ2:
                alg = Bz2Support.getInstance();
                break;
            case DEFLATE:
                alg = DeflateSupport.getInstance();
                break;
            case GZIP:
                alg = GzipSupport.getInstance();
                break;
            case LZ4:
                alg = Lz4Support.getInstance();
                break;
            case SNAPPY:
                alg = SnappySupport.getInstance();
                break;
            case XZ:
                alg = XzSupport.getInstance();
                break;
            case ZSTD:
                alg = ZstdSupport.getInstance();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported decompression algorithm: " + compression);
        }

        return alg;
    }

    default ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
            int level, Runnable postCloseAction) throws IOException {
        throw new UnsupportedOperationException(
                ClickHouseUtils.format(ERROR_UNSUPPORTED_DECOMPRESS_ALG, getAlgorithm()));
    }

    default ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
            int level, Runnable postCloseAction) throws IOException {
        throw new UnsupportedOperationException(ClickHouseUtils.format(ERROR_UNSUPPORTED_COMPRESS_ALG, getAlgorithm()));
    }

    ClickHouseCompression getAlgorithm();
}
