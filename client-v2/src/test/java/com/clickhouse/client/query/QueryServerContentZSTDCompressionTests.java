package com.clickhouse.client.query;

import com.clickhouse.client.api.CompressionMethod;

public class QueryServerContentZSTDCompressionTests extends QueryTests {

    QueryServerContentZSTDCompressionTests() {
        super(true, false, CompressionMethod.ZSTD);
    }
}
