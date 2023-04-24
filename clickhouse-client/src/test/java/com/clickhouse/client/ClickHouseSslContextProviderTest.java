package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseSslContextProviderTest {
    @Test(groups = { "unit" })
    public void testGetProvider() {
        Assert.assertNotNull(ClickHouseSslContextProvider.getProvider());
        Assert.assertEquals(ClickHouseSslContextProvider.getProvider().getClass(),
                ClickHouseSslContextProvider.getProvider().getClass());
    }
}