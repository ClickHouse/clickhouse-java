package com.clickhouse.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseDeferredValueTest {
    @Test(groups = { "unit" })
    public void testDeferredValues() throws Exception {
        final List<Integer> list = new ArrayList<>(2);
        ClickHouseDeferredValue<?> v = ClickHouseDeferredValue.of(list, List.class);
        Assert.assertEquals(v.get(), list);
        list.add(3);
        Assert.assertEquals(v.get(), list);

        v = ClickHouseDeferredValue.of(() -> {
            list.add(5);
            return list;
        });
        Assert.assertEquals(list, Arrays.asList(3));
        Assert.assertEquals(v.get(), list);
        Assert.assertEquals(list, Arrays.asList(3, 5));

        CountDownLatch latch = new CountDownLatch(1);
        v = ClickHouseDeferredValue.of(CompletableFuture.supplyAsync(() -> {
            try {
                latch.await(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            list.remove(0);
            return list;
        }), 500L);
        Assert.assertEquals(list, Arrays.asList(3, 5));
        latch.countDown();
        Thread.sleep(1000L);
        Assert.assertEquals(v.get(), list);
        Assert.assertEquals(list, Arrays.asList(5));
    }

    @Test(groups = { "unit" })
    public void testNullValues() {
        Assert.assertNull(ClickHouseDeferredValue.of((CompletableFuture<?>) null).get());
        Assert.assertNull(ClickHouseDeferredValue.of((CompletableFuture<?>) null, 50L).get());

        Assert.assertNull(ClickHouseDeferredValue.of((Supplier<?>) null).get());

        Assert.assertNull(ClickHouseDeferredValue.of(null, Object.class).get());
        Assert.assertNull(ClickHouseDeferredValue.of(null, null).get());
    }
}
