package com.clickhouse.client.query;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.CompressionMethod;

public class QueryServerContentLZ4CompressionTests extends QueryTests {

    QueryServerContentLZ4CompressionTests() {
        super(true, false, CompressionMethod.LZ4);
    }

    @Override
    protected Client.Builder newClient() {
        return super.newClient()
                .compressionMethod(CompressionMethod.LZ4)
                .serverSetting("network_compression_method", "lz4");
    }
}
