package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseFile;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;

/**
 * Wrapper of {@link java.io.OutputStream}.
 */
public class WrappedOutputStream extends AbstractByteArrayOutputStream {
    private final OutputStream output;

    @Override
    protected void flushBuffer(byte[] bytes, int offset, int length) throws IOException {
        output.write(bytes, offset, length);
    }

    public WrappedOutputStream(ClickHouseFile file, OutputStream stream, int bufferSize, Runnable postCloseAction) {
        super(file, ClickHouseUtils.getBufferSize(bufferSize,
                (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue()), postCloseAction);

        output = ClickHouseChecker.nonNull(stream, "OutputStream");
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        if (position > 0) {
            flushBuffer();
        }
        output.flush();
    }
}
