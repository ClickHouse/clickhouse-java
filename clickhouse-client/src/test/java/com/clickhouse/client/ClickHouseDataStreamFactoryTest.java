package com.clickhouse.client;

import java.util.concurrent.CompletableFuture;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDataStreamFactoryTest {
    @Test(groups = { "unit" })
    public void testGetInstance() throws Exception {
        Assert.assertNotNull(ClickHouseDataStreamFactory.getInstance());
    }

    @Test(groups = { "unit" })
    public void testCreatePipedOutputStream() throws Exception {
        ClickHouseConfig config = new ClickHouseConfig();

        // read in worker thread
        for (int i = 0; i < 256; i++) {
            CompletableFuture<Integer> future;
            try (ClickHousePipedOutputStream out = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(
                    config,
                    null)) {
                future = ClickHouseClient.submit(() -> {
                    try (ClickHouseInputStream in = out.getInputStream()) {
                        return in.read();
                    }
                });
                out.write(i);
            }
            Assert.assertEquals(future.get(), i);
        }

        // write in worker thread
        for (int i = 0; i < 256; i++) {
            ClickHousePipedOutputStream out = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(
                    config,
                    null);
            final int num = i;
            try (ClickHouseInputStream in = out.getInputStream()) {
                ClickHouseClient.submit(() -> {
                    try (ClickHouseOutputStream o = out) {
                        o.write(num);
                    }
                    return num;
                });
                Assert.assertEquals(in.read(), num);
            }
        }
    }
}
