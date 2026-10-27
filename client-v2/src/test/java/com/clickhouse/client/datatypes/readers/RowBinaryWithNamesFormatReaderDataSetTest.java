package com.clickhouse.client.datatypes.readers;

import com.clickhouse.data.ClickHouseFormat;

public class RowBinaryWithNamesFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinaryWithNames;
    }
}
