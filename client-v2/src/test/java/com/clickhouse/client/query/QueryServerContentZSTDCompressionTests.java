package com.clickhouse.client.query;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.CompressionMethod;

public class QueryServerContentZSTDCompressionTests extends QueryTests {

    QueryServerContentZSTDCompressionTests() {
        super(true, false, CompressionMethod.ZSTD);
    }

    @Override
    protected Client.Builder newClient() {
        return super.newClient()
                .compressionMethod(CompressionMethod.ZSTD)
                .serverSetting("network_compression_method", "zstd");
    }
}
