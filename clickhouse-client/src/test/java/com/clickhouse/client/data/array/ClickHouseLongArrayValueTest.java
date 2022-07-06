package com.clickhouse.client.data.array;

import java.math.BigInteger;

import org.junit.Assert;
import org.testng.annotations.Test;

public class ClickHouseLongArrayValueTest {
    @Test(groups = { "unit" })
    public void testConvertToBigInteger() throws Exception {
        ClickHouseLongArrayValue v = ClickHouseLongArrayValue
                .of(new long[] { 1L, new BigInteger("9223372036854775808").longValue() });
        Assert.assertArrayEquals(v.getValue(), new long[] { 1L, -9223372036854775808L });
        Assert.assertArrayEquals(v.asArray(BigInteger.class),
                new BigInteger[] { BigInteger.ONE, new BigInteger("9223372036854775808") });
    }
}
