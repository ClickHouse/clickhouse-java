package com.clickhouse.client.data;

import java.io.EOFException;
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
    protected static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);

    static class Input extends ClickHouseInputStream {
        private final BlockingQueue<ByteBuffer> queue;
        private final int timeout;

        // too much to maintain a 2-level buffer for reading?
        private ByteBuffer buffer;
        private boolean closed;

        Input(BlockingQueue<ByteBuffer> queue, int timeout) {
            this.queue = queue;
            this.timeout = timeout;

            this.buffer = null;
            this.closed = false;
        }

        private void ensureOpen() throws IOException {
            if (this.closed) {
                throw new IOException("Stream has been closed");
            }

            if (this.buffer == null) {
                updateBuffer();
            }
        }

        private int updateBuffer() throws IOException {
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

        @Override
        public int available() throws IOException {
            ensureOpen();

            if (buffer == EMPTY || buffer.limit() == 0) {
                return 0;
            }

            int available = buffer.remaining();
            if (available == 0) {
                available = updateBuffer();
            }

            return available;
        }

        @Override
        public boolean isClosed() {
            return this.closed;
        }

        @Override
        public void close() throws IOException {
            // it's caller's responsiblity to consume all data in the queue, which will
            // unblock writer
            this.closed = true;
            this.buffer = null;
        }

        @Override
        public byte readByte() throws IOException {
            ensureOpen();

            if (buffer == EMPTY || buffer.limit() == 0) {
                close();
                throw new EOFException();
            }

            if (buffer.hasRemaining()) {
                return buffer.get();
            } else {
                updateBuffer();

                return readByte();
            }
        }

        @Override
        public int read() throws IOException {
            ensureOpen();

            if (buffer == EMPTY || buffer.limit() == 0) {
                return -1;
            }

            if (buffer.hasRemaining()) {
                return 0xFF & buffer.get();
            } else {
                updateBuffer();

                return read();
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ensureOpen();

            if (buffer == EMPTY || buffer.limit() == 0) {
                return -1;
            }

            int counter = 0;
            while (len > 0) {
                if (buffer == EMPTY || buffer.limit() == 0) {
                    return counter;
                }

                int remain = buffer.remaining();
                if (remain > 0) {
                    if (remain >= len) {
                        buffer.get(b, off, len);
                        counter += len;
                        len = 0;
                    } else {
                        buffer.get(b, off, remain);
                        counter += remain;
                        off += remain;
                        len -= remain;

                        updateBuffer();
                    }
                } else {
                    updateBuffer();
                }
            }

            return counter;
        }

        @Override
        public long skip(long n) throws IOException {
            ensureOpen();

            // peforms better but this is a bit tricky
            if (n == Long.MAX_VALUE) {
                long counter = buffer.remaining();
                while (buffer != EMPTY && buffer.limit() > 0) {
                    counter += buffer.limit();
                    updateBuffer();
                }

                return counter;
            }

            return super.skip(n);
        }
    }

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
        return new Input(queue, timeout);
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        flush();

        buffer = EMPTY;
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
