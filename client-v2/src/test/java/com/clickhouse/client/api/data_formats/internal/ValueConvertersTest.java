package com.clickhouse.client.api.data_formats.internal;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ValueConvertersTest {

    @Test
    public void testConvertStringToBoolean() {
        ValueConverters vc = new ValueConverters();
        Assert.assertTrue(vc.convertStringToBoolean("1"));
        Assert.assertFalse(vc.convertStringToBoolean("0"));
        Assert.assertTrue(vc.convertStringToBoolean("true"));
        Assert.assertFalse(vc.convertStringToBoolean("false"));
    }
}
