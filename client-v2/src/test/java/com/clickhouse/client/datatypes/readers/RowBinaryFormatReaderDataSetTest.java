package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.data_formats.RowBinaryFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;

public class RowBinaryFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.RowBinary;
    }

    @Override
    protected ClickHouseFormatReader createReader(QueryResponse response, TableSchema schema) {
        return new RowBinaryFormatReader(response.getInputStream(), response.getSettings(), schema,
                new BinaryStreamReader.DefaultByteBufferAllocator());
    }
}
