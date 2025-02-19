package com.clickhouse.data.stream;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;

/**
 * A combination of {@link java.io.PipedOutputStream} and
 * {@link java.io.PipedInputStream} for streaming data between server and
 * client. To avoid dead lock and high memory usage, please make sure writer and
 * reader are on two separate threads.
 */
@Deprecated
public class NonBlockingPipedOutputStream extends ClickHousePipedOutputStream {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingPipedOutputStream.class);

    protected final AdaptiveQueue<ByteBuffer> queue;

    protected final int bufferSize;
    protected final CompletableFuture<Void> future;
    protected final long timeout;

    protected ByteBuffer buffer;

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
        AdaptiveQueue<ByteBuffer> q = queue;
        long t = timeout;
        long startTime = t < 1L ? 0L : System.currentTimeMillis();

        // wait as needed
        while (!q.offer(b)) {
            if (t > 0L && System.currentTimeMillis() - startTime >= t) {
                throw new IOException(ClickHouseUtils.format("Write timed out after %d ms", t));
            }
        }
    }

    public NonBlockingPipedOutputStream(int bufferSize, int queueLength, long timeout, CapacityPolicy policy) {
        this(bufferSize, queueLength, timeout, policy, (Runnable) null);
    }

    public NonBlockingPipedOutputStream(int bufferSize, int queueLength, long timeout, CapacityPolicy policy,
            Runnable postCloseAction) {
        super(postCloseAction);

        this.queue = AdaptiveQueue.create(policy);

        // may need an initialBufferSize and a monitor to update bufferSize in runtime
        this.bufferSize = ClickHouseDataConfig.getBufferSize(bufferSize);
        this.future = ClickHouseUtils.NULL_FUTURE;
        this.timeout = timeout;

        this.buffer = ByteBuffer.allocate(this.bufferSize);
    }

    public NonBlockingPipedOutputStream(int bufferSize, int queueLength, long timeout, CapacityPolicy policy,
            ClickHouseWriter writer) {
        super(null);

        this.queue = AdaptiveQueue.create(policy);

        // may need an initialBufferSize and a monitor to update bufferSize in runtime
        this.bufferSize = ClickHouseDataConfig.getBufferSize(bufferSize);
        this.timeout = timeout;

        this.buffer = ByteBuffer.allocate(this.bufferSize);

        this.future = writeAsync(ClickHouseChecker.nonNull(writer, ClickHouseWriter.TYPE_NAME), this);
    }

    @Override
    public ClickHouseInputStream getInputStream(Runnable postCloseAction) {
        return new NonBlockingInputStream(queue, timeout, () -> handleWriteResult(future, timeout, postCloseAction));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            if (buffer.position() > 0) {
                updateBuffer(false);
            }
        } finally {
            queue.add(ClickHouseByteBuffer.EMPTY_BUFFER);

            closed = true;
            ClickHouseDataStreamFactory.handleCustomAction(postCloseAction);
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

        if (buffer.position() > 0) {
            updateBuffer(true);
        }
        byte[] copy = new byte[length];
        System.arraycopy(bytes, offset, copy, 0, length);
        updateBuffer(ByteBuffer.wrap(copy));

        return this;
    }

    @Override
    public ClickHouseOutputStream writeBuffer(ClickHouseByteBuffer buffer) throws IOException {
        if (buffer == null || buffer.isEmpty()) {
            return this;
        }
        return writeBytes(buffer.array(), buffer.position(), buffer.length());
    }

    @Override
    public ClickHouseOutputStream writeByte(byte b) throws IOException {
        ensureOpen();

        if (!buffer.put(b).hasRemaining()) {
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
            log.debug("writeBytes length:[%d] remain:[%d] offset: [%d]", length, remain, offset);
            if (length < remain) {
                b.put(bytes, offset, length);
                length = 0;
            } else if (b.position() == 0) { // empty buffer
                // it's unsafe to reuse 'bytes' here as it could be a shared buffer
                // updateBuffer(ByteBuffer.wrap(bytes, offset, length));
                byte[] copy = new byte[length];
                System.arraycopy(bytes, offset, copy, 0, length);
                updateBuffer(ByteBuffer.wrap(copy));
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
