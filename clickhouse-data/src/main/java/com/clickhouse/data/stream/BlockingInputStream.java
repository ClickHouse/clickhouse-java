package com.clickhouse.data.stream;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseUtils;

/**
 * {@link java.nio.ByteBuffer} backed input stream with
 * {@link java.util.concurrent.BlockingQueue}.
 */
@Deprecated
public class BlockingInputStream extends AbstractByteBufferInputStream {
    private final BlockingQueue<ByteBuffer> queue;
    private final long timeout;

    public BlockingInputStream(BlockingQueue<ByteBuffer> queue, long timeout, Runnable postCloseAction) {
        super(null, null, postCloseAction);

        this.queue = ClickHouseChecker.nonNull(queue, "Queue");
        this.timeout = timeout < 0L ? 0L : timeout;
    }

    @Override
    protected int updateBuffer() throws IOException {
        ByteBuffer b;
        try {
            if (timeout > 0L) {
                b = queue.poll(timeout, TimeUnit.MILLISECONDS);
                if (b == null) {
                    throw new IOException(ClickHouseUtils.format("Read timed out after %d ms", timeout));
                }
            } else {
                b = queue.take();
            }

            buffer = b;
            int remain = b.remaining();
            if (remain > 0 && copyTo != null) {
                int position = b.position();
                if (b.hasArray()) {
                    copyTo.write(b.array(), position, remain);
                } else {
                    byte[] bytes = new byte[remain];
                    b.get(bytes);
                    copyTo.write(bytes);
                    ((Buffer) b).position(position);
                }
            }
            return remain;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when getting next buffer from queue", e);
        }
    }
}
