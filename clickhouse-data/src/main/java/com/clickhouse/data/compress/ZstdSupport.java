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
public final class ZstdSupport {
    static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        protected int normalize(int level) {
            return level < 0 || level > 22 ? com.github.luben.zstd.Zstd.defaultCompressionLevel() : level;
        }

        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream, new com.github.luben.zstd.ZstdOutputStream(output, normalize(level)),
                    bufferSize, postCloseAction);
        }

        @Override
        @SuppressWarnings("resource")
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream, new com.github.luben.zstd.ZstdInputStream(input).setContinuous(true),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.ZSTD;
        }
    }

    static final class Factory {
        private static final ClickHouseCompressionAlgorithm instance = new DefaultImpl();

        private Factory() {
        }
    }

    public static ClickHouseCompressionAlgorithm getInstance() {
        return Factory.instance;
    }

    private ZstdSupport() {
    }
}
