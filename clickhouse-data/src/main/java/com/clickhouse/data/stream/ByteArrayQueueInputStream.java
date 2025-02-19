package com.clickhouse.data.stream;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;

import java.io.IOException;
import java.util.Queue;

@Deprecated
public class ByteArrayQueueInputStream extends AbstractByteArrayInputStream {

    private final Queue<byte[]> queue;

    public ByteArrayQueueInputStream(Queue<byte[]> queue, Runnable postCloseAction) {
        super(null, null, postCloseAction);
        this.queue =  ClickHouseChecker.nonNull(queue, "queue");;
    }

    @Override
    protected int updateBuffer() throws IOException {
        position = 0;

        while (!queue.isEmpty()) {
            byte[] bytes = queue.poll();
            int len = bytes != null ? bytes.length : 0;
            if (len > 0) {
                buffer = bytes;
                if (copyTo != null) {
                    copyTo.write(bytes);
                }
                return limit = len;
            }
        }

        buffer = ClickHouseByteBuffer.EMPTY_BYTES;
        return limit = 0;
    }
}
