package com.clickhouse.data.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseCompressionAlgorithm;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.stream.WrappedInputStream;
import com.clickhouse.data.stream.WrappedOutputStream;

@Deprecated
public final class SnappySupport {
    public static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream,
                    new org.apache.commons.compress.compressors.snappy.SnappyCompressorOutputStream(output, bufferSize),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream,
                    new org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream(input,
                            normalize(level)),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.SNAPPY;
        }
    }

    public static class PreferredImpl implements ClickHouseCompressionAlgorithm {
        public PreferredImpl() {
            new org.xerial.snappy.Snappy(); // NOSONAR
        }

        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream, new org.xerial.snappy.SnappyOutputStream(output, normalize(level)),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            // https://github.com/ClickHouse/ClickHouse/issues/44885
            return new WrappedInputStream(stream, new org.xerial.snappy.SnappyInputStream(input), bufferSize,
                    postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.SNAPPY;
        }
    }

    static final class Factory {
        private static final ClickHouseCompressionAlgorithm instance = ClickHouseCompressionAlgorithm
                .createInstance("snappy_lib_detection", PreferredImpl.class, DefaultImpl.class);

        private Factory() {
        }
    }

    static final int MIN_BLOCK_SIZE = 1024;
    static final int DEFAULT_BLOCK_SIZE = 32 * 1024;

    private static int normalize(int level) {
        return level < MIN_BLOCK_SIZE ? DEFAULT_BLOCK_SIZE : (level - level % 1024);
    }

    public static ClickHouseCompressionAlgorithm getInstance() {
        return Factory.instance;
    }

    private SnappySupport() {
    }
}
