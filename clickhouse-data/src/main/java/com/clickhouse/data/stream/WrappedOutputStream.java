package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHousePassThruStream;

/**
 * Wrapper of {@link java.io.OutputStream}.
 */
@Deprecated
public class WrappedOutputStream extends AbstractByteArrayOutputStream {
    private final OutputStream output;

    @Override
    protected void flushBuffer(byte[] bytes, int offset, int length) throws IOException {
        output.write(bytes, offset, length);
    }

    public WrappedOutputStream(ClickHousePassThruStream stream, OutputStream out, int bufferSize,
            Runnable postCloseAction) {
        super(stream, ClickHouseDataConfig.getBufferSize(bufferSize), postCloseAction);

        output = ClickHouseChecker.nonNull(out, "OutputStream");
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        if (position > 0) {
            flushBuffer();
        }
        output.flush();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                // flush before closing the inner output stream
                super.close();
            } finally {
                output.close();
            }
        }
    }
}
