package com.clickhouse.client.api.data_formats;

import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseRecord;

import java.util.function.Consumer;

public class RowBinaryReader implements RecordReader {

    private final ClickHouseInputStream inputStream;

    public RowBinaryReader(ClickHouseInputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public boolean readBatch(int size, Consumer<ClickHouseRecord> consumer, Consumer<Exception> errorHandler) {
       // TODO: implementation of record reader will get raw stream from response and will read records from it
        return false;
    }

    @Override
    public boolean canRead(String dataFormat) {
        return "RowBinary".equalsIgnoreCase(dataFormat);
    }
}
