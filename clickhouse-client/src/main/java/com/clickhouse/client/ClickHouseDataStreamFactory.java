package com.clickhouse.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.config.ClickHouseBufferingMode;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.data.ClickHousePipedStream;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor;
import com.clickhouse.client.data.ClickHouseTabSeparatedProcessor;
import com.clickhouse.client.stream.BlockingPipedOutputStream;
import com.clickhouse.client.stream.CapacityPolicy;
import com.clickhouse.client.stream.NonBlockingPipedOutputStream;

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
     * Gets data processor according to given {@link ClickHouseConfig} and settings.
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
    public ClickHouseDataProcessor getProcessor(ClickHouseConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, Map<String, Object> settings, List<ClickHouseColumn> columns)
            throws IOException {
        ClickHouseFormat format = ClickHouseChecker.nonNull(config, "config").getFormat();
        ClickHouseDataProcessor processor = null;
        if (ClickHouseFormat.RowBinary == format || ClickHouseFormat.RowBinaryWithNamesAndTypes == format) {
            processor = new ClickHouseRowBinaryProcessor(config, input, output, columns, settings);
        } else if (ClickHouseFormat.TSVWithNames == format || ClickHouseFormat.TSVWithNamesAndTypes == format
                || ClickHouseFormat.TabSeparatedWithNames == format
                || ClickHouseFormat.TabSeparatedWithNamesAndTypes == format) {
            processor = new ClickHouseTabSeparatedProcessor(config, input, output, columns, settings);
        } else if (format != null && format.isText()) {
            processor = new ClickHouseTabSeparatedProcessor(config, input, output,
                    ClickHouseDataProcessor.DEFAULT_COLUMNS, settings);
        }
        return processor;
    }

    /**
     * Gets deserializer for the given data format.
     *
     * @param format data format, null means
     *               {@code ClickHouseDefaults.FORMAT.getEffectiveDefaultValue()}
     * @return deserializer for the given data format
     */
    public ClickHouseDeserializer<ClickHouseValue> getDeserializer(ClickHouseFormat format) {
        if (format == null) {
            format = (ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue();
        }
        if (!format.supportsInput()) {
            throw new IllegalArgumentException(ClickHouseUtils.format(ERROR_NO_DESERIALIZER, format.name()));
        }

        ClickHouseDeserializer<ClickHouseValue> deserializer;
        if (format.isText()) {
            deserializer = ClickHouseTabSeparatedProcessor.getMappedFunctions(format);
        } else if (format == ClickHouseFormat.RowBinary || format == ClickHouseFormat.RowBinaryWithNamesAndTypes) {
            deserializer = ClickHouseRowBinaryProcessor.getMappedFunctions();
        } else {
            throw new IllegalArgumentException(ERROR_UNSUPPORTED_FORMAT + format);
        }
        return deserializer;
    }

    /**
     * Gets serializer for the given data format.
     *
     * @param format data format, null means
     *               {@code ClickHouseDefaults.FORMAT.getEffectiveDefaultValue()}
     * @return serializer for the given data format
     */
    public ClickHouseSerializer<ClickHouseValue> getSerializer(ClickHouseFormat format) {
        if (format == null) {
            format = (ClickHouseFormat) ClickHouseDefaults.FORMAT.getEffectiveDefaultValue();
        }
        if (!format.supportsOutput()) {
            throw new IllegalArgumentException(ClickHouseUtils.format(ERROR_NO_SERIALIZER, format.name()));
        }

        ClickHouseSerializer<ClickHouseValue> serializer;
        if (format.isText()) {
            serializer = ClickHouseTabSeparatedProcessor.getMappedFunctions(format);
        } else if (format == ClickHouseFormat.RowBinary || format == ClickHouseFormat.RowBinaryWithNamesAndTypes) {
            serializer = ClickHouseRowBinaryProcessor.getMappedFunctions();
        } else {
            throw new IllegalArgumentException(ERROR_UNSUPPORTED_FORMAT + format);
        }
        return serializer;
    }

    /**
     * Creates a piped stream.
     *
     * @param config non-null configuration
     * @return piped stream
     * @deprecated will be removed in v0.3.3, please use
     *             {@link #createPipedOutputStream(ClickHouseConfig, Runnable)}
     *             instead
     */
    @Deprecated
    public ClickHousePipedStream createPipedStream(ClickHouseConfig config) {
        return config != null
                ? new ClickHousePipedStream(config.getWriteBufferSize(), config.getMaxQueuedBuffers(),
                        config.getSocketTimeout())
                : new ClickHousePipedStream((int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                        (int) ClickHouseClientOption.MAX_QUEUED_BUFFERS.getDefaultValue(),
                        (int) ClickHouseClientOption.SOCKET_TIMEOUT.getDefaultValue());
    }

    /**
     * Creates a piped output stream.
     *
     * @param config          non-null configuration
     * @param postCloseAction custom action will be performed right after closing
     *                        the output stream
     * @return piped output stream
     */
    public ClickHousePipedOutputStream createPipedOutputStream(ClickHouseConfig config, Runnable postCloseAction) {
        final int bufferSize = ClickHouseChecker.nonNull(config, "config").getWriteBufferSize();
        final boolean blocking;
        final int queue;
        final CapacityPolicy policy;
        final int timeout;

        if (config.getRequestBuffering() == ClickHouseBufferingMode.PERFORMANCE) {
            blocking = false;
            queue = 0;
            policy = null;
            timeout = 0; // questionable
        } else {
            blocking = config.isUseBlockingQueue();
            queue = config.getMaxQueuedBuffers();
            policy = config.getBufferQueueVariation() < 1 ? CapacityPolicy.fixedCapacity(queue)
                    : CapacityPolicy.linearDynamicCapacity(1, queue, config.getBufferQueueVariation());
            timeout = config.getSocketTimeout();
        }
        return blocking
                ? new BlockingPipedOutputStream(bufferSize, queue, timeout, postCloseAction)
                : new NonBlockingPipedOutputStream(bufferSize, queue, timeout, policy, postCloseAction);
    }

    public ClickHousePipedOutputStream createPipedOutputStream(int bufferSize, int queueSize, int timeout,
            Runnable postCloseAction) {
        return new BlockingPipedOutputStream(
                ClickHouseUtils.getBufferSize(bufferSize,
                        (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue(),
                        (int) ClickHouseClientOption.MAX_BUFFER_SIZE.getDefaultValue()),
                queueSize, timeout, postCloseAction);
    }
}
