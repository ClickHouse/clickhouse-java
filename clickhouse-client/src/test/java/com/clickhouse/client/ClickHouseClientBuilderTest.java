package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseClientBuilder.Agent;
import com.clickhouse.client.config.ClickHouseClientOption;

public class ClickHouseClientBuilderTest {
    @Test(groups = { "unit" })
    public void testBuildClient() {
        ClickHouseClientBuilder builder = new ClickHouseClientBuilder();
        ClickHouseClient client = builder.build();
        Assert.assertTrue(client instanceof Agent);
        Assert.assertTrue(((Agent) client).getClient() instanceof ClickHouseClientBuilder.DummyClient);
        Assert.assertNotEquals(builder.build(), client);
        Assert.assertTrue(client.getConfig() == builder.getConfig());

        builder.nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.ANY));
        client = builder.build();
        Assert.assertTrue(client instanceof Agent);
        Assert.assertTrue(((Agent) client).getClient() instanceof ClickHouseTestClient);
        Assert.assertNotEquals(builder.build(), client);
        Assert.assertTrue(client.getConfig() == builder.getConfig());
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
