package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.clickhouse.client.ClickHouseRequest.Mutation;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;

public class ClickHouseClientTest {
    @Test(groups = { "unit" })
    public void testGetAsyncRequestOutputStream() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        for (int i = 0; i < 256; i++) {
            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            try (ClickHouseOutputStream chOut = ClickHouseClient.getAsyncRequestOutputStream(config, bas, null)) {
                chOut.write(i);
            }
            Assert.assertEquals(bas.toByteArray(), new byte[] { (byte) i });
        }
    }

    @Test(groups = { "unit" })
    public void testGetRequestOutputStream() throws IOException {
        ClickHouseConfig config = new ClickHouseConfig();
        for (int i = 0; i < 256; i++) {
            ByteArrayOutputStream bas = new ByteArrayOutputStream();
            try (ClickHouseOutputStream chOut = ClickHouseClient.getRequestOutputStream(config, bas, null)) {
                chOut.write(i);
            }
            Assert.assertEquals(bas.toByteArray(), new byte[] { (byte) i });
        }
    }

    @Test(groups = { "unit" })
    public void testQuery() throws ExecutionException, InterruptedException {
        ClickHouseClient client = ClickHouseClient.builder().build();
        Assert.assertNotNull(client);
        ClickHouseRequest<?> req = client.read(ClickHouseNode.builder().build());
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
    public void testMutation() throws ExecutionException, InterruptedException {
        ClickHouseClient client = ClickHouseClient.builder().build();
        Assert.assertNotNull(client);
        Mutation req = client.read(ClickHouseNode.builder().build()).write();
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
