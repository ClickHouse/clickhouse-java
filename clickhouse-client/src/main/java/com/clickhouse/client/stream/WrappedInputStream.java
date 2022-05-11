package com.clickhouse.client.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseDataUpdater;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.config.ClickHouseClientOption;

/**
 * Wrapper of {@link java.io.InputStream} with buffer.
 */
public class WrappedInputStream extends AbstractByteArrayInputStream {
    private final InputStream in;

    @Override
    protected int updateBuffer() throws IOException {
        position = 0;

        if (closed) {
            return limit = 0;
        }

        int len = buffer.length;
        int off = 0;
        while (len > 0) {
            int read = in.read(buffer, off, len);
            if (read == -1) {
                break;
            } else {
                off += read;
                len -= read;
            }
        }
        if (copyTo != null) {
            copyTo.write(buffer, 0, off);
        }
        limit = off;
        return limit - position;
    }

    public WrappedInputStream(InputStream input, int bufferSize, Runnable postCloseAction) {
        super(null, postCloseAction);

        in = ClickHouseChecker.nonNull(input, "InputStream");
        // fixed buffer
        buffer = new byte[ClickHouseUtils.getBufferSize(bufferSize,
                (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue())];

        position = 0;
        limit = 0;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            in.close();
        } finally {
            super.close();
        }
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        if (reader == null) {
            return byteBuffer.reset();
        }
        ensureOpen();

        LinkedList<byte[]> list = new LinkedList<>();
        int offset = position;
        int length = 0;
        boolean more = true;
        while (more) {
            int remain = limit - position;
            if (remain < 1) {
                closeQuietly();
                more = false;
            } else {
                int read = reader.update(buffer, position, limit);
                if (read == -1) {
                    byte[] bytes = new byte[limit];
                    System.arraycopy(buffer, position, bytes, position, remain);
                    length += remain;
                    position = limit;
                    list.add(bytes);
                    if (updateBuffer() < 1) {
                        closeQuietly();
                        more = false;
                    }
                } else {
                    length += read;
                    position += read;
                    list.add(buffer);
                    more = false;
                }
            }
        }
        return byteBuffer.update(list, offset, length);
    }

    @Override
    public long pipe(ClickHouseOutputStream output) throws IOException {
        long count = 0L;
        if (output == null || output.isClosed()) {
            return count;
        }
        ensureOpen();

        int l = limit;
        int p = position;
        int remain = l - p;
        if (remain > 0) {
            output.writeBytes(buffer, p, remain);
            count += remain;
            position = l;
        }

        while ((remain = updateBuffer()) > 0) {
            output.writeBytes(buffer, 0, remain);
            count += remain;
        }
        close();
        return count;
    }
}
