package com.clickhouse.data.value.array;

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseLongArrayValueTest {
    @Test(groups = { "unit" })
    public void testConvertToBigInteger() {
        ClickHouseLongArrayValue v = ClickHouseLongArrayValue
                .of(new long[] { 1L, new BigInteger("9223372036854775808").longValue() });
        Assert.assertEquals(v.getValue(), new long[] { 1L, -9223372036854775808L });
        Assert.assertEquals(v.asArray(BigInteger.class),
                new BigInteger[] { BigInteger.ONE, new BigInteger("9223372036854775808") });
    }

    @Test(groups = { "unit" })
    public void testConvertFromBigInteger() {
        ClickHouseLongArrayValue v = ClickHouseLongArrayValue.ofEmpty();
        Assert.assertEquals(v.getValue(), new long[0]);
        v.update(new BigInteger[] { BigInteger.ONE, new BigInteger("9223372036854775808") });
        Assert.assertEquals(v.getValue(), new long[] { 1L, -9223372036854775808L });
    }
}
