package com.clickhouse.client.api;


import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;


public class ClientConfigPropertiesTest {

    @Test(groups = {"unit"})
    public void testToKeyValuePairs() {

        Map<String, String> map = ClientConfigProperties.toKeyValuePairs("key1=value1,key2=value2");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("key1"), "value1");
        Assert.assertEquals(map.get("key2"), "value2");

        map = ClientConfigProperties.toKeyValuePairs("key1=value1, key2 = value2");
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("key1"), "value1");
        Assert.assertEquals(map.get("key2"), "value2");

        map = ClientConfigProperties.toKeyValuePairs("key1");
        Assert.assertEquals(map.size(), 0);

        // TODO: improve implementation
//        map = ClientConfigProperties.toKeyValuePairs("key1=value1, ,key2=value2");
//        Assert.assertEquals(map.size(), 2);
//        Assert.assertEquals(map.get("key1"), "value1");
//        Assert.assertEquals(map.get("key2"), "value2");
    }
}