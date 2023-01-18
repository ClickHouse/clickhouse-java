package com.clickhouse.data;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.clickhouse.config.ClickHouseBufferingMode;
import com.clickhouse.data.format.ClickHouseRowBinaryProcessor;
import com.clickhouse.data.format.ClickHouseTabSeparatedProcessor;
import com.clickhouse.data.stream.BlockingPipedOutputStream;
import com.clickhouse.data.stream.CapacityPolicy;
import com.clickhouse.data.stream.NonBlockingPipedOutputStream;

/**
 * Factory class for creating objects to handle data stream.
 */
public class ClickHouseDataStreamFactory {
    private static final ClickHouseDataStreamFactory instance = ClickHouseUtils
            .getService(ClickHouseDataStreamFactory.class, new ClickHouseDataStreamFactory());

    protected static final String ERROR_NO_DESERIALIZER = "No deserializer available because format %s does not support input";
    protected static final String ERROR_NO_SERIALIZER = "No serializer available because format %s does not support output";
    protected static final String ERROR_UNSUPPORTED_FORMAT = "Unsupported format: ";

    /**
     * Gets instance of the factory class.
     *
     * @return instance of the factory class
     */
    public static ClickHouseDataStreamFactory getInstance() {
        return instance;
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
        if (ClickHouseFormat.RowBinary == format || ClickHouseFormat.RowBinaryWithNamesAndTypes == format) {
            processor = new ClickHouseRowBinaryProcessor(config, input, output, columns, settings);
        } else if (format.isText()) {
            processor = new ClickHouseTabSeparatedProcessor(config, input, output, columns, settings);
        }
        return processor;
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

    public ClickHousePipedOutputStream createPipedOutputStream(int bufferSize, int queueSize, int timeout,
            Runnable postCloseAction) {
        return new BlockingPipedOutputStream(ClickHouseDataConfig.getBufferSize(bufferSize), queueSize, timeout,
                postCloseAction);
    }
}
