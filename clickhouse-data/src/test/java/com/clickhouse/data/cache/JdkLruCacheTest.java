package com.clickhouse.data.cache;

import java.util.HashMap;
import java.util.Map;

import com.clickhouse.data.ClickHouseCache;

import org.testng.Assert;
import org.testng.annotations.Test;

public class JdkLruCacheTest {
    @Test(groups = { "unit" })
    public void testCache() {
        int capacity = 3;
        ClickHouseCache<String, String> cache = JdkLruCache.create(capacity, (k) -> k);
        Assert.assertNotNull(cache);

        Map<String, String> map = (Map<String, String>) cache.unwrap(Map.class);
        Assert.assertNotNull(map);
        Assert.assertEquals(map.size(), 0);

        Map<String, String> m = new HashMap<>();
        m.put("A", "A");
        m.put("B", "B");
        m.put("C", "C");
        Assert.assertEquals(cache.get("A"), "A");
        Assert.assertEquals(map.size(), 1);
        Assert.assertEquals(cache.get("B"), "B");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(cache.get("C"), "C");
        Assert.assertEquals(map.size(), 3);
        Assert.assertEquals(map, m);
        Assert.assertEquals(cache.get("D"), "D");
        Assert.assertEquals(map.size(), 3);
        Assert.assertNotEquals(map, m);
        m.remove("A");
        m.put("D", "D");
        Assert.assertEquals(map, m);
    }
}
