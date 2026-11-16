package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;

public class RowBinaryWithNamesAndTypesFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinaryWithNamesAndTypes;
    }

    @Override
    protected ClickHouseFormatReader createReader(QueryResponse response, TableSchema schema) {
        return new RowBinaryWithNamesAndTypesFormatReader(response.getInputStream(), response.getSettings(),
                new BinaryStreamReader.DefaultByteBufferAllocator());
    }
}
