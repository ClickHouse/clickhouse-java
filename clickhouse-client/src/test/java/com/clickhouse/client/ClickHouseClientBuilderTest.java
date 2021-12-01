package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.config.ClickHouseClientOption;

public class ClickHouseClientBuilderTest {
    @Test(groups = { "unit" })
    public void testBuildClient() {
        ClickHouseClientBuilder builder = new ClickHouseClientBuilder();
        ClickHouseClient client = builder.build();
        Assert.assertTrue(client instanceof ClickHouseTestClient);
        Assert.assertNotEquals(builder.build(), client);

        ClickHouseTestClient testClient = (ClickHouseTestClient) client;
        Assert.assertTrue(testClient.getConfig() == builder.getConfig());
    }

    @Test(groups = { "unit" })
    public void testBuildConfig() {
        ClickHouseClientBuilder builder = new ClickHouseClientBuilder();
        ClickHouseConfig config = builder.getConfig();
        Assert.assertNotNull(config);
        Assert.assertEquals(builder.getConfig(), config);

        String clientName = "test client";
        builder.option(ClickHouseClientOption.CLIENT_NAME, clientName);
        Assert.assertNotEquals(builder.getConfig(), config);
        config = builder.getConfig();
        Assert.assertEquals(config.getClientName(), clientName);
        Assert.assertEquals(config.getOption(ClickHouseClientOption.CLIENT_NAME), clientName);
    }
}
