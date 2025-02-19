package com.clickhouse.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * SPSC(Single-producer single-consumer) channel for streaming.
 */
@Deprecated
public abstract class ClickHousePipedOutputStream extends ClickHouseOutputStream {
    /**
     * Handles async write result.
     * 
     * @param future          async write result
     * @param timeout         timeout in milliseconds
     * @param postCloseAction post close aciton, could be null
     * @throws UncheckedIOException when writing failed
     */
    protected static void handleWriteResult(CompletableFuture<?> future, long timeout, Runnable postCloseAction)
            throws UncheckedIOException {
        try {
            if (future != null) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException("Writing was interrupted", e));
        } catch (TimeoutException e) {
            throw new UncheckedIOException(
                    new IOException(ClickHouseUtils.format("Writing timed out after %d milliseconds", timeout), e));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw new UncheckedIOException((IOException) cause);
            } else if (cause instanceof UncheckedIOException) {
                throw (UncheckedIOException) cause;
            } else {
                throw new UncheckedIOException(new IOException("Writing failed", cause));
            }
        } finally {
            if (postCloseAction != null) {
                postCloseAction.run();
            }
        }
    }

    /**
     * Writes data to the piped output stream in a separate thread. The given piped
     * output stream will be closed automatically at the end of writing.
     *
     * @param writer non-null custom writer
     * @param output non-null piped output stream
     * @return non-null future
     */
    protected static CompletableFuture<Void> writeAsync(ClickHouseWriter writer, ClickHousePipedOutputStream output) {
        return ClickHouseDataStreamFactory.getInstance().runBlockingTask(() -> {
            try (ClickHouseOutputStream out = output) {
                writer.write(out);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
            return null;
        });
    }

    protected ClickHousePipedOutputStream(Runnable postCloseAction) {
        super(null, postCloseAction);
    }

    /**
     * Gets input stream to reada data being written into the output stream.
     *
     * @return non-null input stream
     */
    public final ClickHouseInputStream getInputStream() {
        return getInputStream(null);
    }

    /**
     * Gets input stream to reada data being written into the output stream.
     *
     * @param postCloseAction custom action will be performed right after closing
     *                        the input stream
     * @return non-null input stream
     */
    public abstract ClickHouseInputStream getInputStream(Runnable postCloseAction);
}
