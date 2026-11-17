package com.clickhouse.client.datatypes.readers;

import com.clickhouse.client.api.data_formats.ClickHouseFormatReader;
import com.clickhouse.client.api.data_formats.NativeFormatReader;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.data.ClickHouseFormat;

public class NativeFormatReaderDataSetTest extends AbstractBinaryReaderDataSetTest {

    @Override
    protected ClickHouseFormat getFormat() {
        return ClickHouseFormat.Native;
    }

    @Override
    protected ClickHouseFormatReader createReader(QueryResponse response, TableSchema schema) {
        return new NativeFormatReader(response.getInputStream(), response.getSettings(),
                new BinaryStreamReader.DefaultByteBufferAllocator());
    }
}
