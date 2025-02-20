package com.clickhouse.data;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.data.format.ClickHouseBinaryFormatProcessor;
import com.clickhouse.data.format.ClickHouseRowBinaryProcessor;
import com.clickhouse.data.format.ClickHouseTabSeparatedProcessor;
import com.clickhouse.data.stream.BlockingPipedOutputStream;
import com.clickhouse.data.stream.CapacityPolicy;
import com.clickhouse.data.stream.NonBlockingPipedOutputStream;

/**
 * Factory class for creating objects to handle data stream.
 */
@Deprecated
public class ClickHouseDataStreamFactory {
    protected static final class DefaultExecutors {
        protected static final ExecutorService executor;
        protected static final ScheduledExecutorService scheduler;

        static {
            int coreThreads = 2 * Runtime.getRuntime().availableProcessors() + 1;
            if (coreThreads < ClickHouseUtils.MIN_CORE_THREADS) {
                coreThreads = ClickHouseUtils.MIN_CORE_THREADS;
            }

            executor = ClickHouseUtils.newThreadPool("ClickHouseWorker-", coreThreads, coreThreads, 0, 0, false);
            scheduler = Executors.newSingleThreadScheduledExecutor(new ClickHouseThreadFactory("ClickHouseScheduler-"));
        }

        private DefaultExecutors() {
        }
    }

    private static final ClickHouseDataStreamFactory instance = ClickHouseUtils
            .getService(ClickHouseDataStreamFactory.class, new ClickHouseDataStreamFactory());

    protected static final String ERROR_NO_DESERIALIZER = "No deserializer available because format %s does not support input";
    protected static final String ERROR_NO_SERIALIZER = "No serializer available because format %s does not support output";
    protected static final String ERROR_UNSUPPORTED_FORMAT = "Unsupported format: ";

    /**
     * Handles custom action.
     *
     * @param postCloseAction post close action, could be null
     * @throws IOException when failed to execute post close action
     */
    public static void handleCustomAction(Runnable postCloseAction) throws IOException {
        if (postCloseAction == null) {
            return;
        }

        try {
            postCloseAction.run();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * Gets instance of the factory class.
     *
     * @return instance of the factory class
     */
    public static ClickHouseDataStreamFactory getInstance() {
        return instance;
    }

    /**
     * Gets default executor service for running blocking tasks.
     *
     * @return non-null executor service
     */
    public ExecutorService getExecutor() {
        return DefaultExecutors.executor;
    }

    /**
     * Gets default scheduled executor service for scheduled tasks.
     *
     * @return non-null scheduled executor service
     */
    public ScheduledExecutorService getScheduler() {
        return DefaultExecutors.scheduler;
    }

    /**
     * Executes a blocking task using
     * {@link CompletableFuture#supplyAsync(Supplier)} and custom
     * {@link ExecutorService}.
     *
     * @return non-null future to get result
     */
    public <T> CompletableFuture<T> runBlockingTask(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier, DefaultExecutors.executor);
    }

    /**
     * Schedules a task using
     * {@link ScheduledExecutorService#schedule(Runnable, long, TimeUnit)} and
     * custom {@link ScheduledExecutorService}.
     *
     * @return non-null future to get result
     */
    public ScheduledFuture<?> scheduleTask(Runnable task, long delay, TimeUnit unit) {
        return DefaultExecutors.scheduler.schedule(task, delay, unit);
    }

    /**
     * Gets data processor according to given {@link ClickHouseDataConfig} and
     * settings.
     *
     * @param config   non-null configuration containing information like
     *                 {@link ClickHouseFormat}
     * @param input    input stream for deserialization, must not be null when
     *                 {@code output} is null
     * @param output   output stream for serialization, must not be null when
     *                 {@code input} is null
     * @param settings nullable settings
     * @param columns  nullable list of columns
     * @return data processor, which might be null
     * @throws IOException when failed to read columns from input stream
     */
    public ClickHouseDataProcessor getProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, Map<String, Serializable> settings, List<ClickHouseColumn> columns)
            throws IOException {
        ClickHouseFormat format = ClickHouseChecker.nonNull(config, ClickHouseDataConfig.TYPE_NAME).getFormat();
        ClickHouseDataProcessor processor = null;
        if (ClickHouseFormat.RowBinary == format ||
                ClickHouseFormat.RowBinaryWithNamesAndTypes == format ||
                ClickHouseFormat.RowBinaryWithDefaults == format ||
                ClickHouseFormat.RowBinaryWithNames == format) {
            processor = new ClickHouseRowBinaryProcessor(config, input, output, columns, settings);
        } else if (format.isBinary()) {
            // to let outer code access input stream
            processor = new ClickHouseBinaryFormatProcessor(config, input, output, columns, settings);
        } else if (format.isText()) {
            processor = new ClickHouseTabSeparatedProcessor(config, input, output, columns, settings);
        }
        return processor;
    }

    /**
     * Creates a piped output stream.
     *
     * @param config non-null configuration
     * @return piped output stream
     */
    public final ClickHousePipedOutputStream createPipedOutputStream(ClickHouseDataConfig config) {
        return createPipedOutputStream(config, (Runnable) null);
    }

    /**
     * Creates a piped output stream.
     *
     * @param config          non-null configuration
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return piped output stream
     */
    public ClickHousePipedOutputStream createPipedOutputStream(ClickHouseDataConfig config, Runnable postCloseAction) {
        final int bufferSize = ClickHouseChecker.nonNull(config, ClickHouseDataConfig.TYPE_NAME).getWriteBufferSize();
        final boolean blocking;
        final int queue;
        final CapacityPolicy policy;
        final int timeout;

        if (config.getReadBufferingMode() == ClickHouseBufferingMode.PERFORMANCE) {
            blocking = false;
            queue = 0;
            policy = null;
            timeout = 0; // questionable
        } else {
            blocking = config.isUseBlockingQueue();
            queue = config.getMaxQueuedBuffers();
            policy = config.getBufferQueueVariation() < 1 ? CapacityPolicy.fixedCapacity(queue)
                    : CapacityPolicy.linearDynamicCapacity(1, queue, config.getBufferQueueVariation());
            timeout = config.getReadTimeout();
        }
        return blocking
                ? new BlockingPipedOutputStream(bufferSize, queue, timeout, postCloseAction)
                : new NonBlockingPipedOutputStream(bufferSize, queue, timeout, policy, postCloseAction);
    }

    /**
     * Creates a piped output stream.
     *
     * @param config non-null configuration
     * @param writer non-null custom writer
     * @return piped output stream
     */
    public ClickHousePipedOutputStream createPipedOutputStream(ClickHouseDataConfig config, ClickHouseWriter writer) {
        if (config == null || writer == null) {
            throw new IllegalArgumentException("Non-null config and writer are required");
        }

        final int bufferSize = config.getWriteBufferSize();
        final boolean blocking;
        final int queue;
        final CapacityPolicy policy;
        final int timeout;

        if (config.getReadBufferingMode() == ClickHouseBufferingMode.PERFORMANCE) {
            blocking = false;
            queue = 0;
            policy = null;
            timeout = 0; // questionable
        } else {
            blocking = config.isUseBlockingQueue();
            queue = config.getMaxQueuedBuffers();
            policy = config.getBufferQueueVariation() < 1 ? CapacityPolicy.fixedCapacity(queue)
                    : CapacityPolicy.linearDynamicCapacity(1, queue, config.getBufferQueueVariation());
            timeout = config.getReadTimeout();
        }

        return blocking
                ? new BlockingPipedOutputStream(bufferSize, queue, timeout, writer)
                : new NonBlockingPipedOutputStream(bufferSize, queue, timeout, policy, writer);
    }

    public final ClickHousePipedOutputStream createPipedOutputStream(int bufferSize, int queueSize, int timeout) {
        return createPipedOutputStream(bufferSize, queueSize, timeout, (Runnable) null);
    }

    public ClickHousePipedOutputStream createPipedOutputStream(int bufferSize, int queueSize, int timeout,
            Runnable postCloseAction) {
        return new BlockingPipedOutputStream(ClickHouseDataConfig.getBufferSize(bufferSize), queueSize, timeout,
                postCloseAction);
    }

    public ClickHousePipedOutputStream createPipedOutputStream(int bufferSize, int queueSize, int timeout,
            ClickHouseWriter writer) {
        return new BlockingPipedOutputStream(ClickHouseDataConfig.getBufferSize(bufferSize), queueSize, timeout,
                writer);
    }
}
