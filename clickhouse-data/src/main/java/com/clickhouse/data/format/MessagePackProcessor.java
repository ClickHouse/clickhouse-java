package com.clickhouse.data.format;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseDataProcessor;
import com.clickhouse.data.ClickHouseDeserializer;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseSerializer;
import com.clickhouse.data.ClickHouseValue;

public class MessagePackProcessor extends ClickHouseDataProcessor {

    protected MessagePackProcessor(ClickHouseDataConfig config, ClickHouseInputStream input,
            ClickHouseOutputStream output,
            List<ClickHouseColumn> columns, Map<String, Serializable> settings) throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    protected ClickHouseRecord createRecord() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void readAndFill(ClickHouseValue value) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected List<ClickHouseColumn> readColumns() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void write(ClickHouseValue value) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public ClickHouseDeserializer getDeserializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ClickHouseSerializer getSerializer(ClickHouseDataConfig config, ClickHouseColumn column) {
        // TODO Auto-generated method stub
        return null;
    }
}
