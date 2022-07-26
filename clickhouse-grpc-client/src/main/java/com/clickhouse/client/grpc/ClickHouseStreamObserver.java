package com.clickhouse.client.grpc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.clickhouse.client.ClickHouseCompression;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseDataStreamFactory;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseInputStream;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseOutputStream;
import com.clickhouse.client.ClickHousePipedOutputStream;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.ClickHouseUtils;
import com.clickhouse.client.grpc.impl.Exception;
import com.clickhouse.client.grpc.impl.LogEntry;
import com.clickhouse.client.grpc.impl.Progress;
import com.clickhouse.client.grpc.impl.Result;
import com.clickhouse.client.grpc.impl.Stats;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;

public class ClickHouseStreamObserver implements StreamObserver<Result> {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseStreamObserver.class);

    private final ClickHouseNode server;

    private final CountDownLatch startLatch;
    private final CountDownLatch finishLatch;

    private final ClickHouseOutputStream stream;
    private final ClickHouseInputStream input;

    private final ClickHouseResponseSummary summary;

    private final AtomicReference<IOException> errorRef;

    protected ClickHouseStreamObserver(ClickHouseConfig config, ClickHouseNode server, ClickHouseOutputStream output) {
        this.server = server;

        this.startLatch = new CountDownLatch(1);
        this.finishLatch = new CountDownLatch(1);

        Runnable postCloseAction = () -> {
            IOException exp = getError();
            if (exp != null) {
                throw new UncheckedIOException(exp);
            }
        };
        if (output != null) {
            this.stream = output;
            this.input = ClickHouseInputStream.wrap(null, ClickHouseInputStream.empty(),
                    config.getReadBufferSize(), postCloseAction, ClickHouseCompression.NONE, 0);
        } else {
            ClickHousePipedOutputStream pipedStream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, null);
            this.stream = pipedStream;
            this.input = ClickHouseGrpcResponse.getInput(config, pipedStream.getInputStream(), postCloseAction);
        }

        this.summary = new ClickHouseResponseSummary(null, null);

        this.errorRef = new AtomicReference<>(null);
    }

    protected void checkClosed() {
        if (finishLatch.getCount() == 0) {
            throw new IllegalStateException("closed observer");
        }
    }

    protected boolean updateStatus(Result result) {
        summary.update();

        log.debug(() -> {
            for (LogEntry e : result.getLogsList()) {
                String logLevel = e.getLevel().name();
                int index = logLevel.indexOf('_');
                if (index > 0) {
                    logLevel = logLevel.substring(index + 1);
                }
                log.info("%s.%s [ %s ] {%s} <%s> %s: %s", e.getTime(), e.getTimeMicroseconds(), e.getThreadId(),
                        e.getQueryId(), logLevel, e.getSource(), e.getText());
            }

            return ClickHouseUtils.format("Logged %d entries from server", result.getLogsList().size());
        });

        boolean proceed = true;

        if (result.hasStats()) {
            Stats s = result.getStats();
            summary.update(new ClickHouseResponseSummary.Statistics(s.getRows(), s.getBlocks(), s.getAllocatedBytes(),
                    s.getAppliedLimit(), s.getRowsBeforeLimit()));
        }

        if (result.hasProgress()) {
            Progress p = result.getProgress();
            summary.update(new ClickHouseResponseSummary.Progress(p.getReadRows(), p.getReadBytes(),
                    p.getTotalRowsToRead(), p.getWrittenRows(), p.getWrittenBytes()));
        }

        if (result.getCancelled()) {
            proceed = false;
            onError(new StatusException(Status.CANCELLED));
        } else if (result.hasException()) {
            proceed = false;
            Exception e = result.getException();
            log.error("Server error: Code=%s, %s", e.getCode(), e.getDisplayText());
            log.error(e.getStackTrace());

            Throwable error = errorRef.get();
            if (error == null) {
                errorRef.compareAndSet(null, new IOException(ClickHouseException
                        .buildErrorMessage(result.getException().getCode(), result.getException().getDisplayText())));
            }
        }

        return proceed;
    }

    public boolean isCompleted() {
        return finishLatch.getCount() == 0;
    }

    public boolean isCancelled() {
        return isCompleted() && errorRef.get() != null;
    }

    public ClickHouseResponseSummary getSummary() {
        return summary;
    }

    public IOException getError() {
        return errorRef.get();
    }

    @Override
    public void onNext(Result value) {
        try {
            checkClosed();

            log.trace("Got result: %s", value);

            // consume value in a worker thread might not be helpful
            if (updateStatus(value)) {
                try {
                    // TODO close output stream if value.getOutput().isEmpty()?
                    stream.transferBytes(value.getOutput().toByteArray());
                } catch (IOException e) {
                    onError(e);
                }
            }
        } finally {
            startLatch.countDown();
        }
    }

    @Override
    public void onError(Throwable t) {
        try {
            log.error("Query failed", t);

            errorRef.compareAndSet(null, new IOException(t));
            try {
                stream.close();
            } catch (IOException e) {
                // ignore
            }
            checkClosed();
            // Status status = Status.fromThrowable(error = t);
        } finally {
            startLatch.countDown();
            finishLatch.countDown();
        }
    }

    @Override
    public void onCompleted() {
        log.trace("Query finished");

        try {
            stream.flush();
        } catch (IOException e) {
            errorRef.compareAndSet(null, e);
            log.error("Failed to flush output", e);
        } finally {
            startLatch.countDown();
            finishLatch.countDown();

            try {
                stream.close();
            } catch (IOException e) {
                log.warn("Failed to close output stream", e);
            }
        }
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return startLatch.await(timeout, unit);
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return finishLatch.await(timeout, unit);
    }

    public ClickHouseInputStream getInputStream() {
        return this.input;
    }
}
