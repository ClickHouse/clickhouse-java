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
public final class BrotliSupport {
    public static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream, new org.brotli.dec.BrotliInputStream(input, bufferSize), bufferSize,
                    postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.BROTLI;
        }
    }

    public static class PreferredImpl implements ClickHouseCompressionAlgorithm {
        public PreferredImpl() {
            com.aayushatharva.brotli4j.Brotli4jLoader.ensureAvailability();
        }

        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream, new com.aayushatharva.brotli4j.encoder.BrotliOutputStream(output,
                    new com.aayushatharva.brotli4j.encoder.Encoder.Parameters()
                            .setQuality(normalize(level)),
                    bufferSize), bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream,
                    new com.aayushatharva.brotli4j.decoder.BrotliInputStream(input, bufferSize), bufferSize,
                    postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.BROTLI;
        }
    }

    static final class Factory {
        private static final ClickHouseCompressionAlgorithm instance = ClickHouseCompressionAlgorithm
                .createInstance("brotli_lib_detection", PreferredImpl.class, DefaultImpl.class);

        private Factory() {
        }
    }

    private static int normalize(int level) {
        return level < -1 || level > 11 ? 4 : level;
    }

    public static ClickHouseCompressionAlgorithm getInstance() {
        return Factory.instance;
    }

    private BrotliSupport() {
    }
}
