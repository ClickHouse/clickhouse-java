package com.clickhouse.data.value.array;

import org.junit.Assert;
import org.testng.annotations.Test;

public class ClickHouseByteArrayValueTest {
    @Test(groups = { "unit" })
    public void testConvertToBoolean() {
        ClickHouseByteArrayValue v = ClickHouseByteArrayValue
                .of(new byte[] { 0, 1, -1 });
        Assert.assertArrayEquals(v.getValue(), new byte[] { 0, 1, -1 });
        Assert.assertArrayEquals(v.asArray(Boolean.class), new Boolean[] { false, true, false });
    }

    @Test(groups = { "unit" })
    public void testConvertFromBoolean() {
        ClickHouseByteArrayValue v = ClickHouseByteArrayValue.ofEmpty();
        Assert.assertArrayEquals(v.getValue(), new byte[0]);
        v.update(new boolean[] { false, true, false });
        Assert.assertArrayEquals(v.getValue(), new byte[] { 0, 1, 0 });
        v.resetToNullOrEmpty();
        Assert.assertArrayEquals(v.getValue(), new byte[0]);
        v.update(new Boolean[] { Boolean.FALSE, Boolean.FALSE, Boolean.TRUE });
        Assert.assertArrayEquals(v.getValue(), new byte[] { 0, 0, 1 });
    }
}
