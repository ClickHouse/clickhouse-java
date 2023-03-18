package com.clickhouse.data;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseFreezableMapTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        Assert.assertEquals(ClickHouseFreezableMap.of(new HashMap<String, String>()), new HashMap<>());
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
}