package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHousePassThruStream;

/**
 * Wrapper of {@link java.io.InputStream} with buffer.
 */
@Deprecated
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

    public WrappedInputStream(ClickHousePassThruStream stream, InputStream input, int bufferSize,
            Runnable postCloseAction) {
        super(stream, null, postCloseAction);

        in = ClickHouseChecker.nonNull(input, "InputStream");
        // fixed buffer
        buffer = new byte[ClickHouseDataConfig.getBufferSize(bufferSize)];

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
}
