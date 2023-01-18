package com.clickhouse.data;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseCompressionAlgorithmTest {
    @Test(groups = { "unit" })
    public void testCreateInstance() {
        for (ClickHouseCompression c : ClickHouseCompression.values()) {
            ClickHouseCompressionAlgorithm alg = ClickHouseCompressionAlgorithm.of(c);
            Assert.assertNotNull(alg);
            Assert.assertEquals(alg.getAlgorithm(), c);
        }
    }
}
