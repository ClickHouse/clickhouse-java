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
public final class XzSupport {
    public static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        protected int normalize(int level) {
            return level < 0 || level > 9 ? 6 : level;
        }

        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedOutputStream(stream,
                    new org.tukaani.xz.XZOutputStream(output, new org.tukaani.xz.LZMA2Options(normalize(level)),
                            org.tukaani.xz.XZ.CHECK_CRC64),
                    bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return new WrappedInputStream(stream, new org.tukaani.xz.XZInputStream(input), bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.XZ;
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

    private XzSupport() {
    }
}
