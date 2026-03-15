package com.clickhouse.client.api;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientConfigPropertiesTest {

    @Test
    public void testBooleanParseValue() {
        // HTTP_USE_BASIC_AUTH is defined as Boolean.class
        Object v1 = ClientConfigProperties.HTTP_USE_BASIC_AUTH.parseValue("1");
        Assert.assertTrue(v1 instanceof Boolean && (Boolean) v1);

        Object v0 = ClientConfigProperties.HTTP_USE_BASIC_AUTH.parseValue("0");
        Assert.assertTrue(v0 instanceof Boolean && !((Boolean) v0));

        Object vt = ClientConfigProperties.HTTP_USE_BASIC_AUTH.parseValue("true");
        Assert.assertTrue(vt instanceof Boolean && (Boolean) vt);

        Object vf = ClientConfigProperties.HTTP_USE_BASIC_AUTH.parseValue("false");
        Assert.assertTrue(vf instanceof Boolean && !((Boolean) vf));
    }
}
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