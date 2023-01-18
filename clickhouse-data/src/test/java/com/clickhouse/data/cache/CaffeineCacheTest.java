package com.clickhouse.data.cache;

import java.util.HashMap;
import java.util.Map;

import com.clickhouse.data.ClickHouseCache;
import com.github.benmanes.caffeine.cache.Cache;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CaffeineCacheTest {
    @Test(groups = { "unit" })
    public void testCache() {
        int capacity = 3;
        ClickHouseCache<String, String> cache = CaffeineCache.create(capacity, 1L, (k) -> k);
        Assert.assertNotNull(cache);

        Cache<String, String> c = (Cache<String, String>) cache.unwrap(Cache.class);
        Assert.assertNotNull(c);
        Assert.assertEquals(c.estimatedSize(), 0L);

        Map<String, String> m = new HashMap<>();
        m.put("A", "A");
        m.put("B", "B");
        m.put("C", "C");
        Assert.assertEquals(cache.get("A"), "A");
        Assert.assertEquals(c.asMap().size(), 1);
        Assert.assertEquals(cache.get("B"), "B");
        Assert.assertEquals(c.asMap().size(), 2);
        Assert.assertEquals(cache.get("C"), "C");
        Assert.assertEquals(c.asMap().size(), 3);
        Assert.assertEquals(c.asMap(), m);

        try {
            Thread.sleep(1500L);
        } catch (InterruptedException e) {
            Assert.fail("Sleep was interrupted", e);
        }
        c.cleanUp();

        Assert.assertEquals(cache.get("D"), "D");
        Assert.assertEquals(c.asMap().size(), 1);
        Assert.assertNotEquals(c.asMap(), m);
        m.clear();
        m.put("D", "D");
        Assert.assertEquals(c.asMap(), m);
    }
}
