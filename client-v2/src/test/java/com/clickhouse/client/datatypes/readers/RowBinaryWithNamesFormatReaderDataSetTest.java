package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryWithNamesFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;

public class RowBinaryWithNamesFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinaryWithNames;
    }

    @Override
    protected ClickHouseFormatReader createReader(QueryResponse response, TableSchema schema) {
        return new RowBinaryWithNamesFormatReader(response.getInputStream(), response.getSettings(), schema,
                new BinaryStreamReader.DefaultByteBufferAllocator());
    }
}
