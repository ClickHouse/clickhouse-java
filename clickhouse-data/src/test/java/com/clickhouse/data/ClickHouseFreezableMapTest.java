package com.clickhouse.data;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseFreezableMapTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertEquals(ClickHouseFreezableMap.of(new HashMap<String, String>()), new HashMap<>());
        Assert.assertEquals(ClickHouseFreezableMap.of(new HashMap<String, String>(), (String) null), new HashMap<>());
        Assert.assertEquals(ClickHouseFreezableMap.of(new HashMap<String, String>(), "3", null, "1"), new HashMap<>());
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseFreezableMap.of(null));
    }

    @Test(groups = { "unit" })
    public void testFreezeAndUnfreeze() {
        Map<String, String> map = new HashMap<>();
        ClickHouseFreezableMap<String, String> freezableMap = ClickHouseFreezableMap.of(map);

        Assert.assertEquals(freezableMap, map);

        freezableMap.put("a", null);
        freezableMap.put(null, "2");
        Assert.assertEquals(freezableMap, map);

        freezableMap.freeze().put("c", "3");
        Assert.assertEquals(freezableMap.size(), 2);
        Assert.assertEquals(freezableMap, map);

        freezableMap.unfreeze().put("d", "4");
        Assert.assertEquals(freezableMap.size(), 3);
        Assert.assertEquals(freezableMap, map);
        Assert.assertEquals(freezableMap.get("d"), "4");
    }

    @Test(groups = { "unit" })
    public void testWhiteList() {
        Map<String, String> map = new HashMap<>();
        ClickHouseFreezableMap<String, String> freezableMap = ClickHouseFreezableMap.of(map, "5", "6", "7");

        Assert.assertTrue(freezableMap.isWhiteListed("5"));
        Assert.assertTrue(freezableMap.isWhiteListed("6"));
        Assert.assertTrue(freezableMap.isWhiteListed("7"));

        Assert.assertFalse(freezableMap.isWhiteListed("a"));
        Assert.assertFalse(freezableMap.isWhiteListed(null));
        Assert.assertFalse(freezableMap.isWhiteListed("b"));
        Assert.assertFalse(freezableMap.isWhiteListed("c"));
        Assert.assertFalse(freezableMap.isWhiteListed("d"));

        Assert.assertEquals(freezableMap, map);

        freezableMap.put("a", null);
        freezableMap.put(null, "2");
        Assert.assertEquals(freezableMap, map);

        freezableMap.freeze().put("c", "3");
        Assert.assertEquals(freezableMap.size(), 2);
        Assert.assertEquals(freezableMap, map);
        freezableMap.put("5", "?");
        Assert.assertEquals(freezableMap.size(), 3);
        Assert.assertEquals(freezableMap, map);
        Assert.assertEquals(freezableMap.get("5"), "?");
        freezableMap.remove("5");
        Assert.assertEquals(freezableMap.size(), 2);
        Assert.assertEquals(freezableMap, map);

        freezableMap.unfreeze().put("d", "4");
        Assert.assertEquals(freezableMap.size(), 3);
        Assert.assertEquals(freezableMap, map);
        Assert.assertEquals(freezableMap.get("d"), "4");
        freezableMap.put("5", "67");
        Assert.assertEquals(freezableMap.size(), 4);
        Assert.assertEquals(freezableMap, map);
        Assert.assertEquals(freezableMap.get("5"), "67");
    }
}