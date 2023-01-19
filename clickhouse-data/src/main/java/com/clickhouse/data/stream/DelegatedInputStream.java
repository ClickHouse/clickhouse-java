package com.clickhouse.data.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.imageio.IIOException;

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
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.data.ClickHouseWriter;

public class DelegatedInputStream extends ClickHouseInputStream {
    private final ClickHouseInputStream input;

    private final int timeout;
    private final CompletableFuture<Boolean> future;

    public DelegatedInputStream(ClickHousePassThruStream stream, ClickHouseInputStream input, OutputStream copyTo,
            Runnable postCloseAction) {
        super(stream, copyTo, postCloseAction);

        this.input = ClickHouseChecker.nonNull(input, TYPE_NAME);
        this.timeout = ClickHouseDataConfig.DEFAULT_TIMEOUT;
        this.future = CompletableFuture.completedFuture(true);
    }

    public DelegatedInputStream(ClickHouseDataConfig config, ClickHouseWriter writer) {
        super(null, null, null);

        if (writer == null) {
            throw new IllegalArgumentException("Non-null writer is required");
        }

        this.timeout = config != null ? config.getReadTimeout() : ClickHouseDataConfig.DEFAULT_TIMEOUT;
        ClickHousePipedOutputStream stream = config != null
                ? ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config, null) // NOSONAR
                : ClickHouseDataStreamFactory.getInstance().createPipedOutputStream( // NOSONAR
                        ClickHouseDataConfig.DEFAULT_WRITE_BUFFER_SIZE, 0, this.timeout, null);
        this.input = stream.getInputStream();
        this.future = CompletableFuture.supplyAsync(() -> {
            try (ClickHouseOutputStream out = stream) {
                writer.write(out);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
            return true;
        });
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
        try {
            try {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Custom writer was interrupted", e);
            } catch (TimeoutException e) {
                throw new IIOException(
                        ClickHouseUtils.format("Custom writing timed out after %d milliseconds", timeout), e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw ((IOException) cause);
                } else if (cause instanceof UncheckedIOException) {
                    throw ((UncheckedIOException) cause).getCause();
                } else {
                    throw new IOException("Custom writing failure", cause);
                }
            }
        } finally {
            try {
                super.close();
            } finally {
                input.close();
            }
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