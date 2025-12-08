package com.clickhouse.client.internal;

import com.clickhouse.client.api.data_formats.internal.SerializerUtils;
import org.testng.annotations.Test;

import org.testng.Assert;

public class SerializerUtilsTests {

    @Test
    public void testConvertToInteger() {
        int expected = 1640995199; // Unix timestamp for the given date
        Assert.assertEquals(SerializerUtils.convertToInteger("1640995199").intValue(), expected);
        Assert.assertEquals(SerializerUtils.convertToInteger(false).intValue(), 0);
    }
}
