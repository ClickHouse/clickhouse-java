package com.clickhouse.client.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.clickhouse.client.ClickHouseByteBuffer;
import com.clickhouse.client.ClickHouseChecker;

public class IterableByteBufferInputStream extends AbstractByteBufferInputStream {
    private final Iterator<ByteBuffer> it;

    public IterableByteBufferInputStream(Iterable<ByteBuffer> source, Runnable postCloseAction) {
        super(postCloseAction);

        it = ClickHouseChecker.nonNull(source, "Source").iterator();
    }

    @Override
    protected int updateBuffer() throws IOException {
        while (it.hasNext()) {
            ByteBuffer bytes = it.next();
            if (bytes != null && bytes.hasRemaining()) {
                buffer = bytes;
                return bytes.remaining();
            }
        }
        buffer = ClickHouseByteBuffer.EMPTY_BUFFER;
        return 0;
    }
}
