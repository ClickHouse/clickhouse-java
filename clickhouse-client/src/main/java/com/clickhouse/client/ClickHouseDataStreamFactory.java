package com.clickhouse.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import com.clickhouse.client.data.ClickHousePipedStream;
import com.clickhouse.client.data.ClickHouseRowBinaryProcessor;
import com.clickhouse.client.data.ClickHouseTabSeparatedProcessor;

/**
 * Factory class for creating objects to handle data stream.
 */
public class ClickHouseDataStreamFactory {
    private static final ClickHouseDataStreamFactory instance = ClickHouseUtils
            .getService(ClickHouseDataStreamFactory.class, new ClickHouseDataStreamFactory());

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
    public ClickHouseDataProcessor getProcessor(ClickHouseConfig config, InputStream input, OutputStream output,
            Map<String, Object> settings, List<ClickHouseColumn> columns) throws IOException {
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
            throw new IllegalArgumentException("Unsupported format: " + format);
        }

        return processor;
    }

    /**
     * Creates a piped stream.
     *
     * @param config non-null configuration
     * @return piped stream
     */
    public ClickHousePipedStream createPipedStream(ClickHouseConfig config) {
        ClickHouseChecker.nonNull(config, "config");

        return new ClickHousePipedStream(config.getMaxBufferSize(), config.getMaxQueuedBuffers(),
                config.getSocketTimeout());
    }
}
