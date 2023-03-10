package com.clickhouse.client.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDefaultSslContextProviderTest {
    @Test(groups = { "unit" })
    public void testGetAlgorithm() {
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("", null), null);
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("---BEGIN ", "x"), "x");
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("---BEGIN PRIVATE KEY---", "x"), "x");
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("---BEGIN  PRIVATE KEY---", "x"), "x");
        Assert.assertEquals(ClickHouseDefaultSslContextProvider.getAlgorithm("-----BEGIN RSA PRIVATE KEY-----", ""),
                "RSA");
    }

    @Test(groups = { "unit" })
    public void testGetPrivateKey() throws Exception {
        // openssl genpkey -out pkey4test.pem -algorithm RSA -pkeyopt rsa_keygen_bits:2048
        Assert.assertNotNull(ClickHouseDefaultSslContextProvider.getPrivateKey("pkey4test.pem"));
    }
}