package com.clickhouse.data.stream;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
public class BlockingPipedOutputStream extends ClickHousePipedOutputStream {
    protected final BlockingQueue<ByteBuffer> queue;

    private final int bufferSize;
    private final int timeout;

    private ByteBuffer buffer;

    public BlockingPipedOutputStream(int bufferSize, int queueLength, int timeout, Runnable postCloseAction) {
        super(postCloseAction);

        // DisruptorBlockingQueue? Did not see much difference here...
        this.queue = queueLength <= 0 ? new LinkedBlockingQueue<>() : new ArrayBlockingQueue<>(queueLength);

        // may need an initialBufferSize and a monitor to update bufferSize in runtime
        this.bufferSize = ClickHouseDataConfig.getBufferSize(bufferSize);
        this.timeout = timeout;

        this.buffer = ByteBuffer.allocate(this.bufferSize);
    }

    private void updateBuffer(boolean allocateNewBuffer) throws IOException {
        ByteBuffer b = buffer;
        if (b.hasRemaining()) {
            ((Buffer) b).limit(b.position());
        }
        ((Buffer) b).rewind();

        updateBuffer(b);

        if (allocateNewBuffer) {
            buffer = ByteBuffer.allocate(bufferSize);
        }
    }

    private void updateBuffer(ByteBuffer b) throws IOException {
        try {
            if (timeout > 0) {
                if (!queue.offer(b, timeout, TimeUnit.MILLISECONDS)) {
                    throw new IOException(ClickHouseUtils.format("Write timed out after %d ms", timeout));
                }
            } else {
                queue.put(b);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread was interrupted when putting buffer into queue", e);
        }
    }

    @Override
    public ClickHouseInputStream getInputStream(Runnable postCloseAction) {
        return new BlockingInputStream(queue, timeout, postCloseAction);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (buffer.position() > 0) {
            updateBuffer(false);
        }

        buffer = ClickHouseByteBuffer.EMPTY_BUFFER;
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
        } finally {
            closed = true;
            if (postCloseAction != null) {
                postCloseAction.run();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();

        if (buffer.position() > 0) {
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

        ByteBuffer b = buffer;
        if (b.position() > 0) {
            updateBuffer(true);
        }
        updateBuffer(ByteBuffer.wrap(bytes, offset, length));

        return this;
    }

    @Override
    public ClickHouseOutputStream writeByte(byte b) throws IOException {
        ensureOpen();

        buffer.put(b);
        if (!buffer.hasRemaining()) {
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

        ByteBuffer b = buffer;
        while (length > 0) {
            int remain = b.remaining();
            if (length < remain) {
                b.put(bytes, offset, length);
                length = 0;
            } else if (b.position() == 0) {
                // buffer = ByteBuffer.wrap(bytes, offset, length);
                buffer = ByteBuffer.allocate(length);
                buffer.put(bytes, offset, length);
                updateBuffer(false);
                buffer = b;
                length = 0;
            } else {
                b.put(bytes, offset, remain);
                offset += remain;
                length -= remain;
                updateBuffer(true);
                b = buffer;
            }
        }

        return this;
    }

    @Override
    public ClickHouseOutputStream writeCustom(ClickHouseDataUpdater writer) throws IOException {
        ensureOpen();

        int position = 0;
        int written = 0;
        do {
            position = buffer.position();
            int limit = buffer.limit();
            byte[] bytes;
            if (buffer.hasArray()) {
                bytes = buffer.array();
            } else {
                bytes = new byte[limit - position];
                buffer.get(bytes);
            }
            written = writer.update(bytes, position, limit);
            if (written < 0) {
                ((Buffer) buffer).position(limit);
                updateBuffer(true);
            }
        } while (written < 0);
        ((Buffer) buffer).position(position + written);
        return this;
    }
}
