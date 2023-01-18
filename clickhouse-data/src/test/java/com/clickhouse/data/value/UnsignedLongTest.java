package com.clickhouse.data.value;

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnsignedLongTest {
    @Test(groups = { "unit" })
    public void testConversion() {
        Assert.assertEquals(UnsignedLong.ZERO.byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedLong.ZERO.shortValue(), (short) 0);
        Assert.assertEquals(UnsignedLong.ZERO.intValue(), 0);
        Assert.assertEquals(UnsignedLong.ZERO.longValue(), 0L);
        Assert.assertEquals(UnsignedLong.ZERO.floatValue(), 0F);
        Assert.assertEquals(UnsignedLong.ZERO.doubleValue(), 0D);
        Assert.assertEquals(UnsignedLong.ZERO.bigIntegerValue(), BigInteger.ZERO);
        Assert.assertEquals(UnsignedLong.ZERO.toString(), "0");

        Assert.assertEquals(UnsignedLong.ONE.byteValue(), (byte) 1);
        Assert.assertEquals(UnsignedLong.ONE.shortValue(), (short) 1);
        Assert.assertEquals(UnsignedLong.ONE.intValue(), 1);
        Assert.assertEquals(UnsignedLong.ONE.longValue(), 1L);
        Assert.assertEquals(UnsignedLong.ONE.floatValue(), 1F);
        Assert.assertEquals(UnsignedLong.ONE.doubleValue(), 1D);
        Assert.assertEquals(UnsignedLong.ONE.bigIntegerValue(), BigInteger.ONE);
        Assert.assertEquals(UnsignedLong.ONE.toString(), "1");

        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).shortValue(), (short) 0);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).intValue(), 0);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).longValue(), -9223372036854775808L);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).floatValue(), 9223372036854775808F);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).doubleValue(), 9223372036854775808D);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).bigIntegerValue(),
                new BigInteger("9223372036854775808"));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).toString(), "9223372036854775808");

        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).byteValue(), (byte) -1);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).shortValue(), (short) -1);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).intValue(), -1);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).longValue(), 9223372036854775807L);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).floatValue(), 9223372036854775807F);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).doubleValue(), 9223372036854775807D);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).bigIntegerValue(),
                new BigInteger("9223372036854775807"));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).toString(), "9223372036854775807");

        Assert.assertEquals(UnsignedLong.MAX_VALUE.byteValue(), (byte) 255);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.shortValue(), (short) 65535);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.intValue(), (int) 4294967295L);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.longValue(), -1L);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.floatValue(), 18446744073709551615F);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.doubleValue(), 18446744073709551615D);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.bigIntegerValue(), new BigInteger("18446744073709551615"));
        Assert.assertEquals(UnsignedLong.MAX_VALUE.toString(), "18446744073709551615");
    }

    @Test(groups = { "unit" })
    public void testValueOf() {
        Assert.assertTrue(UnsignedLong.ZERO == UnsignedLong.MIN_VALUE);
        Assert.assertTrue(UnsignedLong.ZERO == UnsignedLong.valueOf(0L));
        Assert.assertTrue(UnsignedLong.ONE == UnsignedLong.valueOf(1L));
        Assert.assertTrue(UnsignedLong.TWO == UnsignedLong.valueOf(2L));
        Assert.assertTrue(UnsignedLong.TEN == UnsignedLong.valueOf(10L));
        Assert.assertTrue(UnsignedLong.MAX_VALUE == UnsignedLong.valueOf(-1L));

        Assert.assertTrue(UnsignedLong.ZERO == UnsignedLong.valueOf((BigInteger) null));
        Assert.assertTrue(UnsignedLong.ZERO == UnsignedLong.valueOf(BigInteger.ZERO));
        Assert.assertTrue(UnsignedLong.ONE == UnsignedLong.valueOf(BigInteger.ONE));
        Assert.assertTrue(UnsignedLong.TWO == UnsignedLong.valueOf(new BigInteger("2")));
        Assert.assertTrue(UnsignedLong.TEN == UnsignedLong.valueOf(BigInteger.TEN));
        Assert.assertTrue(UnsignedLong.MAX_VALUE == UnsignedLong.valueOf(new BigInteger("18446744073709551615")));

        Assert.assertTrue(UnsignedLong.ZERO == UnsignedLong.valueOf("0"));
        Assert.assertTrue(UnsignedLong.ONE == UnsignedLong.valueOf("1"));
        Assert.assertTrue(UnsignedLong.TWO == UnsignedLong.valueOf("2"));
        Assert.assertTrue(UnsignedLong.TEN == UnsignedLong.valueOf("10"));
        Assert.assertTrue(UnsignedLong.MAX_VALUE == UnsignedLong.valueOf("18446744073709551615"));

        Assert.assertTrue(UnsignedLong.MIN_VALUE == UnsignedLong.valueOf(new BigInteger("18446744073709551616")));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedLong.valueOf((String) null));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedLong.valueOf(""));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedLong.valueOf("-1"));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedLong.valueOf("18446744073709551616"));
    }

    @Test(groups = { "unit" })
    public void testAdd() {
        Assert.assertEquals(UnsignedLong.ZERO.add(null), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ZERO.add(UnsignedLong.MIN_VALUE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ONE.add(UnsignedLong.ONE), UnsignedLong.TWO);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.add(UnsignedLong.ONE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.add(UnsignedLong.MAX_VALUE),
                UnsignedLong.valueOf("18446744073709551614"));

        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).add(UnsignedLong.ZERO),
                UnsignedLong.valueOf("9223372036854775808"));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).add(UnsignedLong.ZERO),
                UnsignedLong.valueOf(Long.MAX_VALUE));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).add(UnsignedLong.ONE),
                UnsignedLong.valueOf(Long.MIN_VALUE));
    }

    @Test(groups = { "unit" })
    public void testSubtract() {
        Assert.assertEquals(UnsignedLong.ZERO.subtract(null), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ZERO.subtract(UnsignedLong.MIN_VALUE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ONE.subtract(UnsignedLong.ONE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.TWO.subtract(UnsignedLong.ONE), UnsignedLong.ONE);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.subtract(UnsignedLong.ONE),
                UnsignedLong.valueOf("18446744073709551614"));
        Assert.assertEquals(UnsignedLong.MAX_VALUE.subtract(UnsignedLong.MAX_VALUE), UnsignedLong.ZERO);

        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).subtract(UnsignedLong.ZERO),
                UnsignedLong.valueOf("9223372036854775808"));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).subtract(UnsignedLong.ZERO),
                UnsignedLong.valueOf(Long.MAX_VALUE));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).subtract(UnsignedLong.ONE),
                UnsignedLong.valueOf(Long.MAX_VALUE - 1));
    }

    @Test(groups = { "unit" })
    public void testMultiply() {
        Assert.assertEquals(UnsignedLong.ONE.multiply(null), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ONE.multiply(UnsignedLong.MIN_VALUE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ONE.multiply(UnsignedLong.ONE), UnsignedLong.ONE);
        Assert.assertEquals(UnsignedLong.TWO.multiply(UnsignedLong.ONE), UnsignedLong.TWO);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.multiply(UnsignedLong.ONE), UnsignedLong.MAX_VALUE);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.multiply(UnsignedLong.MAX_VALUE), UnsignedLong
                .valueOf(new BigInteger("18446744073709551615").multiply(new BigInteger("18446744073709551615"))));

        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).multiply(UnsignedLong.ZERO), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).multiply(UnsignedLong.ZERO), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).multiply(UnsignedLong.ONE),
                UnsignedLong.valueOf(Long.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testDivide() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedLong.ONE.divide(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedLong.ONE.divide(UnsignedLong.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedLong.ONE.divide(UnsignedLong.MIN_VALUE));

        Assert.assertEquals(UnsignedLong.ONE.divide(UnsignedLong.ONE), UnsignedLong.ONE);
        Assert.assertEquals(UnsignedLong.TWO.divide(UnsignedLong.ONE), UnsignedLong.TWO);
        Assert.assertEquals(UnsignedLong.ONE.divide(UnsignedLong.TWO), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.divide(UnsignedLong.ONE), UnsignedLong.MAX_VALUE);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.divide(UnsignedLong.MAX_VALUE), UnsignedLong.ONE);

        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).divide(UnsignedLong.ONE),
                UnsignedLong.valueOf(Long.MIN_VALUE));
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).divide(UnsignedLong.ONE),
                UnsignedLong.valueOf(Long.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testRemainder() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedLong.ONE.remainder(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedLong.ONE.remainder(UnsignedLong.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedLong.ONE.remainder(UnsignedLong.MIN_VALUE));

        Assert.assertEquals(UnsignedLong.ONE.remainder(UnsignedLong.ONE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.TWO.remainder(UnsignedLong.ONE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.ONE.remainder(UnsignedLong.TWO), UnsignedLong.ONE);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.remainder(UnsignedLong.ONE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.MAX_VALUE.remainder(UnsignedLong.MAX_VALUE), UnsignedLong.ZERO);

        Assert.assertEquals(UnsignedLong.valueOf(Long.MIN_VALUE).remainder(UnsignedLong.ONE), UnsignedLong.ZERO);
        Assert.assertEquals(UnsignedLong.valueOf(Long.MAX_VALUE).remainder(UnsignedLong.ONE), UnsignedLong.ZERO);
    }
}
