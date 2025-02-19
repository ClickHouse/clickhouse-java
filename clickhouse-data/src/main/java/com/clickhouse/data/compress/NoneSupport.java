package com.clickhouse.data.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseCompressionAlgorithm;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.stream.EmptyInputStream;
import com.clickhouse.data.stream.EmptyOutputStream;
import com.clickhouse.data.stream.WrappedInputStream;
import com.clickhouse.data.stream.WrappedOutputStream;

@Deprecated
public final class NoneSupport {
    public static class DefaultImpl implements ClickHouseCompressionAlgorithm {
        @Override
        public ClickHouseOutputStream compress(ClickHousePassThruStream stream, OutputStream output, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return output != EmptyOutputStream.INSTANCE && output instanceof ClickHouseOutputStream
                    ? (ClickHouseOutputStream) output
                    : new WrappedOutputStream(stream, output, bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseInputStream decompress(ClickHousePassThruStream stream, InputStream input, int bufferSize,
                int level, Runnable postCloseAction) throws IOException {
            return input != EmptyInputStream.INSTANCE && input instanceof ClickHouseInputStream
                    ? (ClickHouseInputStream) input
                    : new WrappedInputStream(stream, input, bufferSize, postCloseAction);
        }

        @Override
        public ClickHouseCompression getAlgorithm() {
            return ClickHouseCompression.NONE;
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

    private NoneSupport() {
    }
}
