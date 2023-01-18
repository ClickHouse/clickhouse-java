package com.clickhouse.data.value;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnsignedIntegerTest {
    @Test(groups = { "unit" })
    public void testConversion() {
        Assert.assertEquals(UnsignedInteger.ZERO.byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedInteger.ZERO.shortValue(), (short) 0);
        Assert.assertEquals(UnsignedInteger.ZERO.intValue(), 0);
        Assert.assertEquals(UnsignedInteger.ZERO.longValue(), 0L);
        Assert.assertEquals(UnsignedInteger.ZERO.floatValue(), 0F);
        Assert.assertEquals(UnsignedInteger.ZERO.doubleValue(), 0D);
        Assert.assertEquals(UnsignedInteger.ZERO.toString(), "0");

        Assert.assertEquals(UnsignedInteger.ONE.byteValue(), (byte) 1);
        Assert.assertEquals(UnsignedInteger.ONE.shortValue(), (short) 1);
        Assert.assertEquals(UnsignedInteger.ONE.intValue(), 1);
        Assert.assertEquals(UnsignedInteger.ONE.longValue(), 1L);
        Assert.assertEquals(UnsignedInteger.ONE.floatValue(), 1F);
        Assert.assertEquals(UnsignedInteger.ONE.doubleValue(), 1D);
        Assert.assertEquals(UnsignedInteger.ONE.toString(), "1");

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).shortValue(), (short) 0);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).intValue(), -2147483648);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).longValue(), 2147483648L);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).floatValue(), 2147483648F);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).doubleValue(), 2147483648D);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).toString(), "2147483648");

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).byteValue(), (byte) -1);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).shortValue(), (short) -1);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).intValue(), 2147483647);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).longValue(), 2147483647L);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).floatValue(), 2147483647F);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).doubleValue(), 2147483647D);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).toString(), "2147483647");

        Assert.assertEquals(UnsignedInteger.MAX_VALUE.byteValue(), (byte) 255);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.shortValue(), (short) 65535);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.intValue(), -1);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.longValue(), 4294967295L);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.floatValue(), 4294967295F);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.doubleValue(), 4294967295D);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.toString(), "4294967295");
    }

    @Test(groups = { "unit" })
    public void testValueOf() {
        Assert.assertTrue(UnsignedInteger.ZERO == UnsignedInteger.MIN_VALUE);
        Assert.assertTrue(UnsignedInteger.ZERO == UnsignedInteger.valueOf(0));
        Assert.assertTrue(UnsignedInteger.ONE == UnsignedInteger.valueOf(1));
        Assert.assertTrue(UnsignedInteger.TWO == UnsignedInteger.valueOf(2));
        Assert.assertTrue(UnsignedInteger.TEN == UnsignedInteger.valueOf(10));
        Assert.assertTrue(UnsignedInteger.MAX_VALUE == UnsignedInteger.valueOf(-1));
        // for (long i = 0; i <= 0xFFFFFFFFL; i++) {
        // Assert.assertEquals(UnsignedInteger.valueOf((int) i),
        // UnsignedInteger.valueOf(String.valueOf(i)));
        // Assert.assertEquals(UnsignedInteger.valueOf((int) i).longValue(), i);
        // Assert.assertEquals(UnsignedInteger.valueOf((int) i).toString(),
        // String.valueOf(i));
        // }

        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInteger.valueOf((String) null));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInteger.valueOf(""));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInteger.valueOf("-1"));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedInteger.valueOf("4294967296"));
    }

    @Test(groups = { "unit" })
    public void testAdd() {
        Assert.assertEquals(UnsignedInteger.ZERO.add(null), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ZERO.add(UnsignedInteger.MIN_VALUE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ONE.add(UnsignedInteger.ONE), UnsignedInteger.valueOf(2));
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.add(UnsignedInteger.ONE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.add(UnsignedInteger.MAX_VALUE), UnsignedInteger.valueOf(-2));

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).add(UnsignedInteger.ZERO),
                UnsignedInteger.valueOf("2147483648"));
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).add(UnsignedInteger.ZERO),
                UnsignedInteger.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).add(UnsignedInteger.ONE),
                UnsignedInteger.valueOf("2147483648"));
    }

    @Test(groups = { "unit" })
    public void testSubtract() {
        Assert.assertEquals(UnsignedInteger.ZERO.subtract(null), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ZERO.subtract(UnsignedInteger.MIN_VALUE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ONE.subtract(UnsignedInteger.ONE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.valueOf(2).subtract(UnsignedInteger.ONE), UnsignedInteger.ONE);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.subtract(UnsignedInteger.ONE),
                UnsignedInteger.valueOf("4294967294"));
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.subtract(UnsignedInteger.MAX_VALUE), UnsignedInteger.ZERO);

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).subtract(UnsignedInteger.ZERO),
                UnsignedInteger.valueOf("2147483648"));
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).subtract(UnsignedInteger.ZERO),
                UnsignedInteger.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).subtract(UnsignedInteger.ONE),
                UnsignedInteger.valueOf((Integer.MAX_VALUE - 1)));
    }

    @Test(groups = { "unit" })
    public void testMultiply() {
        Assert.assertEquals(UnsignedInteger.ONE.multiply(null), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ONE.multiply(UnsignedInteger.MIN_VALUE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ONE.multiply(UnsignedInteger.ONE), UnsignedInteger.ONE);
        Assert.assertEquals(UnsignedInteger.valueOf(3).multiply(UnsignedInteger.ONE),
                UnsignedInteger.valueOf(3));
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.multiply(UnsignedInteger.ONE), UnsignedInteger.MAX_VALUE);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.multiply(UnsignedInteger.MAX_VALUE), UnsignedInteger
                .valueOf((int) (0xFFFFFFFFL * 0xFFFFFFFFL)));

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).multiply(UnsignedInteger.ZERO),
                UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).multiply(UnsignedInteger.ZERO),
                UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).multiply(UnsignedInteger.ONE),
                UnsignedInteger.valueOf(Integer.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testDivide() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInteger.ONE.divide(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInteger.ONE.divide(UnsignedInteger.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInteger.ONE.divide(UnsignedInteger.MIN_VALUE));

        Assert.assertEquals(UnsignedInteger.ONE.divide(UnsignedInteger.ONE), UnsignedInteger.ONE);
        Assert.assertEquals(UnsignedInteger.valueOf(3).divide(UnsignedInteger.ONE),
                UnsignedInteger.valueOf(3));
        Assert.assertEquals(UnsignedInteger.ONE.divide(UnsignedInteger.valueOf(3)), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.divide(UnsignedInteger.ONE), UnsignedInteger.MAX_VALUE);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.divide(UnsignedInteger.MAX_VALUE), UnsignedInteger.ONE);

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).divide(UnsignedInteger.ONE),
                UnsignedInteger.valueOf(Integer.MIN_VALUE));
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).divide(UnsignedInteger.ONE),
                UnsignedInteger.valueOf(Integer.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testRemainder() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInteger.ONE.remainder(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInteger.ONE.remainder(UnsignedInteger.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedInteger.ONE.remainder(UnsignedInteger.MIN_VALUE));

        Assert.assertEquals(UnsignedInteger.ONE.remainder(UnsignedInteger.ONE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.valueOf(3).remainder(UnsignedInteger.ONE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.ONE.remainder(UnsignedInteger.valueOf(3)), UnsignedInteger.ONE);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.remainder(UnsignedInteger.ONE), UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.MAX_VALUE.remainder(UnsignedInteger.MAX_VALUE), UnsignedInteger.ZERO);

        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MIN_VALUE).remainder(UnsignedInteger.ONE),
                UnsignedInteger.ZERO);
        Assert.assertEquals(UnsignedInteger.valueOf(Integer.MAX_VALUE).remainder(UnsignedInteger.ONE),
                UnsignedInteger.ZERO);
    }
}
