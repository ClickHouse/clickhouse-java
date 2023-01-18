package com.clickhouse.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDataStreamFactoryTest {
    @Test(groups = { "unit" })
    public void testGetInstance() {
        Assert.assertNotNull(ClickHouseDataStreamFactory.getInstance());
    }

    @Test(groups = { "unit" })
    public void testCreatePipedOutputStream() throws ExecutionException, IOException, InterruptedException {
        ClickHouseDataConfig config = new ClickHouseTestDataConfig();

        // read in worker thread
        for (int i = 0; i < 256; i++) {
            CompletableFuture<Integer> future;
            try (ClickHousePipedOutputStream out = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, null)) {
                future = CompletableFuture.supplyAsync(() -> {
                    try (ClickHouseInputStream in = out.getInputStream()) {
                        return in.read();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                out.write(i);
            }
            Assert.assertEquals(future.get(), i);
        }

        // write in worker thread
        for (int i = 0; i < 256; i++) {
            ClickHousePipedOutputStream out = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config,
                    null);
            final int num = i;
            try (ClickHouseInputStream in = out.getInputStream()) {
                CompletableFuture.supplyAsync(() -> {
                    try (ClickHouseOutputStream o = out) {
                        o.write(num);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return num;
                });
                Assert.assertEquals(in.read(), num);
            }
        }
    }
}
