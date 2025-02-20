package com.clickhouse.data.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseCompressionAlgorithm;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.stream.WrappedInputStream;
import com.clickhouse.data.stream.WrappedOutputStream;

@Deprecated
public final class GzipSupport {
    public static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream, new GZIPOutputStream(output), bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream, new GZIPInputStream(input), bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.GZIP;
        }
    }

    public static class PreferredImpl implements ClickHouseCompressionAlgorithm {
        public PreferredImpl() {
            new org.apache.commons.compress.compressors.gzip.GzipParameters();
        }

        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            org.apache.commons.compress.compressors.gzip.GzipParameters params = new org.apache.commons.compress.compressors.gzip.GzipParameters();
            params.setBufferSize(bufferSize);
            params.setCompressionLevel(normalize(level));
            return new WrappedOutputStream(stream,
                    new org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream(output, params),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream,
                    new org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream(input, true),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.GZIP;
        }
    }

    static final class Factory {
        private static final ClickHouseCompressionAlgorithm instance = ClickHouseCompressionAlgorithm
                .createInstance("gzip_lib_detection", PreferredImpl.class, DefaultImpl.class);

        private Factory() {
        }
    }

    private static int normalize(int level) {
        return level < -1 || level > 9 ? Deflater.DEFAULT_COMPRESSION : level;
    }

    public static ClickHouseCompressionAlgorithm getInstance() {
        return Factory.instance;
    }

    private GzipSupport() {
    }
}
