package com.clickhouse.data.stream;

import java.io.IOException;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseUtils;

/**
 * A combination of {@link java.io.PipedOutputStream} and
 * {@link java.io.PipedInputStream} for streaming data between server and
 * client. To avoid dead lock and high memory usage, please make sure writer and
 * reader are on two separate threads.
 */
public class NonBlockingPipedOutputStream extends ClickHousePipedOutputStream {
    protected final AdaptiveQueue<byte[]> queue;

    protected final int bufferSize;
    protected final int timeout;
    protected final byte[][] buckets;

    protected int current;

    protected byte[] buffer;
    protected int position;

    private byte[] allocateBuffer() {
        position = 0;
        byte[] b;
        if (buckets.length - queue.size() > 1) {
            b = buckets[current];
            if (b == null) {
                b = new byte[bufferSize];
                buckets[current] = b;
            }

            if (++current >= buckets.length) {
                current = 0;
            }
        } else {
            b = new byte[bufferSize];
        }
        return b;
    }

    private void updateBuffer(boolean allocateNewBuffer) throws IOException {
        updateBuffer(buffer, 0, position);

        if (allocateNewBuffer) {
            buffer = allocateBuffer();
        } else {
            position = 0;
        }
    }

    private void updateBuffer(byte[] bytes, int offset, int length) throws IOException {
        byte[] b;
        if (length < buffer.length) {
            b = new byte[length];
            System.arraycopy(bytes, offset, b, 0, length);
        } else {
            b = bytes;
        }

        AdaptiveQueue<byte[]> q = queue;
        long t = timeout;
        long startTime = t < 1L ? 0L : System.currentTimeMillis();

        // wait as needed
        while (!q.offer(b)) {
            if (t > 0L && System.currentTimeMillis() - startTime >= t) {
                throw new IOException(ClickHouseUtils.format("Write timed out after %d ms", t));
            }
        }
    }

    public NonBlockingPipedOutputStream(int bufferSize, int queueLength, int timeout, CapacityPolicy policy,
            Runnable postCloseAction) {
        super(postCloseAction);

        this.queue = AdaptiveQueue.create(policy);

        // may need an initialBufferSize and a monitor to update bufferSize in runtime
        this.bufferSize = ClickHouseDataConfig.getBufferSize(bufferSize);
        this.timeout = timeout;
        this.buckets = queueLength < 2 ? new byte[0][] : new byte[queueLength][];

        this.current = queueLength < 2 ? -1 : 0;
        this.buffer = allocateBuffer();
    }

    @Override
    public ClickHouseInputStream getInputStream(Runnable postCloseAction) {
        return new NonBlockingInputStream(queue, timeout, postCloseAction);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            if (position > 0) {
                updateBuffer(false);
            }
        } finally {
            queue.add(buffer = ClickHouseByteBuffer.EMPTY_BYTES);
            for (int i = 0, len = buckets.length; i < len; i++) {
                buckets[i] = null;
            }

            closed = true;
            if (postCloseAction != null) {
                postCloseAction.run();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        if (position > 0) {
            updateBuffer(true);
        }
    }

    @Override
    public ClickHouseOutputStream transferBytes(byte[] bytes, int offset, int length) throws IOException {
        if (bytes == null) {
            throw new NullPointerException();
        } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        } else if (length == 0) {
            return this;
        }
        ensureOpen();

        if (position > 0) {
            updateBuffer(true);
        }
        updateBuffer(bytes, offset, length);

        return this;
    }

    @Override
    public ClickHouseOutputStream writeByte(byte b) throws IOException {
        ensureOpen();

        buffer[position++] = b;
        if (position >= buffer.length) {
            updateBuffer(true);
        }
        return this;
    }

    @Override
    public ClickHouseOutputStream writeBytes(byte[] bytes, int offset, int length) throws IOException {
        if (bytes == null) {
            throw new NullPointerException();
        } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
            throw new IndexOutOfBoundsException();
        } else if (length == 0) {
            return this;
        }
        ensureOpen();

        while (length > 0) {
            int limit = buffer.length;
            int remain = limit - position;
            if (length < remain) {
                System.arraycopy(bytes, offset, buffer, position, length);
                position += length;
                length = 0;
            } else {
                System.arraycopy(bytes, offset, buffer, position, remain);
                position = limit;
                offset += remain;
                length -= remain;
                updateBuffer(true);
            }
        }
        return this;
    }

    @Override
    public ClickHouseOutputStream writeCustom(ClickHouseDataUpdater writer) throws IOException {
        ensureOpen();

        int written = 0;
        while ((written = writer.update(buffer, position, buffer.length)) < 0) {
            position = buffer.length;
            updateBuffer(true);
        }
        position += written;
        return this;
    }
}
