package com.clickhouse.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.data.ClickHousePipedStream;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor;
import com.clickhouse.client.data.ClickHouseTabSeparatedProcessor;

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
     * @return data processor
     * @throws IOException when failed to read columns from input stream
     */
    public ClickHouseDataProcessor getProcessor(ClickHouseConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output, Map<String, Object> settings, List<ClickHouseColumn> columns)
            throws IOException {
        ClickHouseFormat format = ClickHouseChecker.nonNull(config, "config").getFormat();
        ClickHouseDataProcessor processor;
        if (ClickHouseFormat.RowBinary == format || ClickHouseFormat.RowBinaryWithNamesAndTypes == format) {
            processor = new ClickHouseRowBinaryProcessor(config, input, output, columns, settings);
        } else if (ClickHouseFormat.TSVWithNames == format || ClickHouseFormat.TSVWithNamesAndTypes == format
                || ClickHouseFormat.TabSeparatedWithNames == format
                || ClickHouseFormat.TabSeparatedWithNamesAndTypes == format) {
            processor = new ClickHouseTabSeparatedProcessor(config, input, output, columns, settings);
        } else if (format != null && format.isText()) {
            processor = new ClickHouseTabSeparatedProcessor(config, input, output,
                    ClickHouseDataProcessor.DEFAULT_COLUMNS, settings);
        } else {
            throw new IllegalArgumentException(ERROR_UNSUPPORTED_FORMAT + format);
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
     */
    public ClickHousePipedStream createPipedStream(ClickHouseConfig config) {
        ClickHouseChecker.nonNull(config, "config");

        return new ClickHousePipedStream(config.getWriteBufferSize(), config.getMaxQueuedBuffers(),
                config.getSocketTimeout());
    }
}
