package com.clickhouse.client.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

/**
 * {@link java.nio.ByteBuffer} backed input stream with
 * {@link java.util.concurrent.BlockingQueue}.
 */
public class BlockingInputStream extends AbstractByteBufferInputStream {
    private static final Logger log = LoggerFactory.getLogger(BlockingInputStream.class);

    private final BlockingQueue<ByteBuffer> queue;
    private final int timeout;

    public BlockingInputStream(BlockingQueue<ByteBuffer> queue, int timeout, Runnable postCloseAction) {
        super(postCloseAction);

        this.queue = ClickHouseChecker.nonNull(queue, "Queue");
        this.timeout = timeout > 0 ? timeout : 0;
    }

    @Override
    protected void ensureOpen() throws IOException {
        if (closed) {
            log.debug("Blocking input stream(queue: %d, buffer: %d) has been closed",
                    queue.size(), buffer != null ? buffer.remaining() : 0);
        }
        super.ensureOpen();
    }

    @Override
    protected int updateBuffer() throws IOException {
        try {
            if (timeout > 0) {
                buffer = queue.poll(timeout, TimeUnit.MILLISECONDS);
                if (buffer == null) {
                    throw new IOException(ClickHouseUtils.format("Read timed out after %d ms", timeout));
                }
            } else {
                buffer = queue.take();
            }

            return buffer.remaining();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when getting next buffer from queue", e);
        }
    }
}
