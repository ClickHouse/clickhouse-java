package com.clickhouse.data.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseCompressionAlgorithm;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.stream.WrappedInputStream;
import com.clickhouse.data.stream.WrappedOutputStream;

@Deprecated
public final class DeflateSupport {
    public static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream, new DeflaterOutputStream(output), bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream, new InflaterInputStream(input), bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.DEFLATE;
        }
    }

    public static class PreferredImpl implements ClickHouseCompressionAlgorithm {
        public PreferredImpl() {
            new org.apache.commons.compress.compressors.deflate.DeflateParameters();
        }

        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            org.apache.commons.compress.compressors.deflate.DeflateParameters params = new org.apache.commons.compress.compressors.deflate.DeflateParameters();
            params.setWithZlibHeader(true);
            params.setCompressionLevel(normalize(level));
            return new WrappedOutputStream(stream,
                    new org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream(output, params),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            org.apache.commons.compress.compressors.deflate.DeflateParameters params = new org.apache.commons.compress.compressors.deflate.DeflateParameters();
            params.setWithZlibHeader(true);
            params.setCompressionLevel(normalize(level));
            return new WrappedInputStream(stream,
                    new org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream(input, params),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.DEFLATE;
        }
    }

    static final class Factory {
        private static final ClickHouseCompressionAlgorithm instance = ClickHouseCompressionAlgorithm
                .createInstance("deflate_lib_detection", PreferredImpl.class, DefaultImpl.class);

        private Factory() {
        }
    }

    private static int normalize(int level) {
        return level < Deflater.NO_COMPRESSION || level > Deflater.BEST_COMPRESSION ? Deflater.BEST_SPEED : level;
    }

    public static ClickHouseCompressionAlgorithm getInstance() {
        return Factory.instance;
    }

    private DeflateSupport() {
    }
}
