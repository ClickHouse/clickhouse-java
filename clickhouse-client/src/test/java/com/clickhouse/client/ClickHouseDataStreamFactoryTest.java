package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDataStreamFactoryTest {
    @Test(groups = { "unit" })
    public void testGetInstance() throws Exception {
        Assert.assertNotNull(ClickHouseDataStreamFactory.getInstance());
    }
}
