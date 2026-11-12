package com.clickhouse.client.datatypes.readers;

import com.clickhouse.data.ClickHouseFormat;

public class RowBinaryWithNamesAndTypesFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinaryWithNamesAndTypes;
    }
}
