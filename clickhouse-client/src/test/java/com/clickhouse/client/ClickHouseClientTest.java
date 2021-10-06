package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.ClickHouseRequest.Mutation;

public class ClickHouseClientTest {
    @Test(groups = { "unit" })
    public void testQuery() throws Exception {
        ClickHouseClient client = ClickHouseClient.builder().build();
        Assert.assertNotNull(client);
        ClickHouseRequest<?> req = client.connect(ClickHouseNode.builder().build());
        Assert.assertNotNull(req);
        Assert.assertNull(req.config);
        Assert.assertNotNull(req.getConfig());
        Assert.assertNotNull(req.config);
        Assert.assertEquals(req.getClient(), client);
        Assert.assertEquals(req.getFormat(), client.getConfig().getFormat());
        Assert.assertNull(req.sql);
        Assert.assertNull(req.query("select 1").execute().get());
    }

    @Test(groups = { "unit" })
    public void testMutation() throws Exception {
        ClickHouseClient client = ClickHouseClient.builder().build();
        Assert.assertNotNull(client);
        Mutation req = client.connect(ClickHouseNode.builder().build()).write();
        Assert.assertNotNull(req);
        Assert.assertNull(req.config);
        Assert.assertNotNull(req.getConfig());
        Assert.assertNotNull(req.config);
        Assert.assertEquals(req.getClient(), client);
        Assert.assertEquals(req.getFormat(), client.getConfig().getFormat());
        Assert.assertNull(req.sql);
        Assert.assertNull(req.table("my_table").format(ClickHouseFormat.RowBinary).execute().get());
    }
}
