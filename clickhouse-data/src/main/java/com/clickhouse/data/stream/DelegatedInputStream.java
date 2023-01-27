package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.OutputStream;

import com.clickhouse.data.ClickHouseByteBuffer;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseDataUpdater;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseWriter;

public class DelegatedInputStream extends ClickHouseInputStream {
    private final ClickHouseInputStream input;

    public DelegatedInputStream(ClickHousePassThruStream stream, ClickHouseInputStream input, OutputStream copyTo,
            Runnable postCloseAction) {
        super(stream, copyTo, postCloseAction);

        this.input = ClickHouseChecker.nonNull(input, TYPE_NAME);
    }

    public DelegatedInputStream(ClickHouseDataConfig config, ClickHouseWriter writer) {
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

    public DelegatedInputStream(ClickHouseWriter writer) {
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
    public ClickHouseByteBuffer readCustom(ClickHouseDataUpdater reader) throws IOException {
        return input.readCustom(reader);
    }

    @Override
    public int read() throws IOException {
        return input.read();
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
}