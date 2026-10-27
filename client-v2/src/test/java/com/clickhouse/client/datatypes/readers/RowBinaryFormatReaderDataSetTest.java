package com.clickhouse.client.datatypes.readers;

import com.clickhouse.data.ClickHouseFormat;

public class RowBinaryFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinary;
    }
}
