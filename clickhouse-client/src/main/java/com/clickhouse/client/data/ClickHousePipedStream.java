package com.clickhouse.client.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseUtils;

/**
 * A combination of {@link java.io.PipedOutputStream} and
 * {@link java.io.PipedInputStream} for streaming data between server and
 * client. To avoid dead lock and high memory usage, please make sure writer and
 * reader are on two separate threads.
 */
public class ClickHousePipedStream extends OutputStream {
    protected final BlockingQueue<ByteBuffer> queue;

    private final int bufferSize;
    private final int timeout;

    private ByteBuffer buffer;
    private boolean closed;

    public ClickHousePipedStream(int bufferSize, int queueLength, int timeout) {
        // DisruptorBlockingQueue? Did not see much difference here...
        this.queue = queueLength <= 0 ? new LinkedBlockingDeque<>() : new ArrayBlockingQueue<>(queueLength);

        // may need an initialBufferSize and a monitor to update bufferSize in runtime
        this.bufferSize = bufferSize <= 0 ? 8192 : bufferSize;
        this.timeout = timeout;

        this.buffer = ByteBuffer.allocate(this.bufferSize);
        this.closed = false;
    }

    private void ensureOpen() throws IOException {
        if (this.closed) {
            throw new IOException("Stream has been closed");
        }
    }

    private void updateBuffer() throws IOException {
        if (buffer.position() > 0) {
            if (buffer.hasRemaining()) {
                ((Buffer) buffer).limit(buffer.position());
            }
            ((Buffer) buffer).rewind();

            try {
                if (timeout > 0) {
                    if (!queue.offer(buffer, timeout, TimeUnit.MILLISECONDS)) {
                        throw new IOException(ClickHouseUtils.format("Write timed out after %d ms", timeout));
                    }
                } else {
                    queue.put(buffer);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread was interrupted when putting buffer into queue", e);
            }

            buffer = ByteBuffer.allocate(bufferSize);
        }
    }

    public ClickHouseInputStream getInput() {
        return ClickHouseInputStream.of(queue, timeout);
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        flush();

        buffer = ClickHouseInputStream.EMPTY;
        try {
            if (timeout > 0) {
                if (!queue.offer(buffer, timeout, TimeUnit.MILLISECONDS)) {
                    throw new IOException(ClickHouseUtils.format("Close stream timed out after %d ms", timeout));
                }
            } else {
                queue.put(buffer);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when putting EMPTY buffer into queue", e);
        }
        this.closed = true;
    }

    @Override
    public void flush() throws IOException {
        updateBuffer();
    }

    @Override
    public void write(int b) throws IOException {
        ensureOpen();

        if (!buffer.hasRemaining()) {
            updateBuffer();
        }

        buffer.put((byte) (0xFF & b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureOpen();

        while (len > 0) {
            int remain = buffer.remaining();
            if (remain > 0) {
                if (remain >= len) {
                    buffer.put(b, off, len);
                    len = 0;
                } else {
                    buffer.put(b, off, remain);
                    off += remain;
                    len -= remain;

                    updateBuffer();
                }
            } else {
                updateBuffer();
            }
        }
    }
}
