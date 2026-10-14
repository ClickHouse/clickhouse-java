package com.clickhouse.client.query;

import com.clickhouse.client.api.CompressionMethod;

public class BinaryReadyReusesBuffersTests extends QueryTests  {

    public BinaryReadyReusesBuffersTests() {
        super(false, false, true, CompressionMethod.LZ4);
    }
}
