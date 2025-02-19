package com.clickhouse.data;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

@Deprecated
final class DelegatedInputStream extends ClickHouseInputStream {
    private final ClickHouseInputStream input;

    @Override
    protected ClickHouseByteBuffer getBuffer() {
        return input.getBuffer();
    }

    @Override
    protected ClickHouseByteBuffer nextBuffer() throws IOException {
        return input.nextBuffer();
    }

    DelegatedInputStream(ClickHousePassThruStream stream, ClickHouseInputStream input, OutputStream copyTo,
            Runnable postCloseAction) {
        super(stream, copyTo, postCloseAction);

        this.input = ClickHouseChecker.nonNull(input, TYPE_NAME);
    }

    DelegatedInputStream(ClickHouseDataConfig config, ClickHouseWriter writer) {
        super(null, null, null);

        if (writer == null) {
            throw new IllegalArgumentException("Non-null writer is required");
        }

        final ClickHousePipedOutputStream stream;
        if (config != null) {
            stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config, writer); // NOSONAR
        } else {
            stream = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream( // NOSONAR
                    ClickHouseDataConfig.DEFAULT_WRITE_BUFFER_SIZE, 0, ClickHouseDataConfig.DEFAULT_TIMEOUT, writer);
        }
        this.input = stream.getInputStream();
    }

    DelegatedInputStream(ClickHouseWriter writer) {
        this(null, writer);
    }

    @Override
    public int peek() throws IOException {
        return input.peek();
    }

    @Override
    public long pipe(ClickHouseOutputStream output) throws IOException {
        return input.pipe(output);
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public ClickHouseByteBuffer readBufferUntil(byte[] separator) throws IOException {
        return input.readBufferUntil(separator);
    }

    @Override
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        return input.readCustom(reader);
    }

    @Override
    public int read() throws IOException {
        return input.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return input.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            input.close();
        } finally {
            super.close();
        }
    }

    @Override
    public ClickHouseFile getUnderlyingFile() {
        return input.getUnderlyingFile();
    }

    @Override
    public ClickHousePassThruStream getUnderlyingStream() {
        return input.getUnderlyingStream();
    }

    @Override
    public boolean isClosed() {
        return input.isClosed();
    }

    @Override
    public int available() throws IOException {
        return input.available();
    }

    @Override
    public long skip(long n) throws IOException {
        return input.skip(n);
    }

    @Override
    public Iterator<ClickHouseByteBuffer> iterator() {
        return input.iterator();
    }
}