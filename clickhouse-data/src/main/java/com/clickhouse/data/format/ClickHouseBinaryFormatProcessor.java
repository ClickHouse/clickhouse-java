package com.clickhouse.data.format;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Deprecated
public class ClickHouseBinaryFormatProcessor extends ClickHouseDataProcessor {


    /**
     * Default constructor.
     *
     * @param config   non-null confinguration contains information like format
     * @param input    input stream for deserialization, can be null when
     *                 {@code output} is available
     * @param output   outut stream for serialization, can be null when
     *                 {@code input} is available
     * @param columns  nullable columns
     * @param settings nullable settings
     * @throws IOException when failed to read columns from input stream
     */
    public ClickHouseBinaryFormatProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
                                           ClickHouseOutputStream output, List<ClickHouseColumn> columns,
                                           Map<String, Serializable> settings) throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        throw new UnsupportedOperationException("Native format is not supported yet");
    }

    @Override
    public ClickHouseDeserializer getDeserializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        throw new UnsupportedOperationException("Native format is not supported yet");
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        throw new UnsupportedOperationException("Native format is not supported yet");
    }
}
