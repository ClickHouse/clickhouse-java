package com.clickhouse.client.query;

import com.clickhouse.client.api.CompressionMethod;

public class QueryServerContentLZ4CompressionTests extends QueryTests {

    QueryServerContentLZ4CompressionTests() {
        super(true, false, CompressionMethod.LZ4);
    }
}
