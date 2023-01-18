package com.clickhouse.data.value;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnsignedShortTest {
    @Test(groups = { "unit" })
    public void testConversion() {
        Assert.assertEquals(UnsignedShort.ZERO.byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedShort.ZERO.shortValue(), (short) 0);
        Assert.assertEquals(UnsignedShort.ZERO.intValue(), 0);
        Assert.assertEquals(UnsignedShort.ZERO.longValue(), 0L);
        Assert.assertEquals(UnsignedShort.ZERO.floatValue(), 0F);
        Assert.assertEquals(UnsignedShort.ZERO.doubleValue(), 0D);
        Assert.assertEquals(UnsignedShort.ZERO.toString(), "0");

        Assert.assertEquals(UnsignedShort.ONE.byteValue(), (byte) 1);
        Assert.assertEquals(UnsignedShort.ONE.shortValue(), (short) 1);
        Assert.assertEquals(UnsignedShort.ONE.intValue(), 1);
        Assert.assertEquals(UnsignedShort.ONE.longValue(), 1L);
        Assert.assertEquals(UnsignedShort.ONE.floatValue(), 1F);
        Assert.assertEquals(UnsignedShort.ONE.doubleValue(), 1D);
        Assert.assertEquals(UnsignedShort.ONE.toString(), "1");

        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).shortValue(), (short) -32768);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).intValue(), 32768);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).longValue(), 32768L);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).floatValue(), 32768F);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).doubleValue(), 32768D);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).toString(), "32768");

        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).byteValue(), (byte) -1);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).shortValue(), (short) 32767);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).intValue(), 32767);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).longValue(), 32767L);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).floatValue(), 32767F);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).doubleValue(), 32767D);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).toString(), "32767");

        Assert.assertEquals(UnsignedShort.MAX_VALUE.byteValue(), (byte) 255);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.shortValue(), (short) -1);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.intValue(), 65535);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.longValue(), 65535L);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.floatValue(), 65535F);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.doubleValue(), 65535D);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.toString(), "65535");
    }

    @Test(groups = { "unit" })
    public void testValueOf() {
        Assert.assertTrue(UnsignedShort.ZERO == UnsignedShort.MIN_VALUE);
        for (int i = 0; i <= 65535; i++) {
            Assert.assertEquals(UnsignedShort.valueOf((short) i), UnsignedShort.valueOf(String.valueOf(i)));
            Assert.assertEquals(UnsignedShort.valueOf((short) i).shortValue(), (short) i);
            Assert.assertEquals(UnsignedShort.valueOf((short) i).intValue(), i);
            Assert.assertEquals(UnsignedShort.valueOf((short) i).toString(), String.valueOf(i));
        }

        Assert.assertThrows(NumberFormatException.class, () -> UnsignedShort.valueOf((String) null));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedShort.valueOf(""));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedShort.valueOf("-1"));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedShort.valueOf("65536"));
    }

    @Test(groups = { "unit" })
    public void testAdd() {
        Assert.assertEquals(UnsignedShort.ZERO.add(null), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ZERO.add(UnsignedShort.MIN_VALUE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ONE.add(UnsignedShort.ONE), UnsignedShort.valueOf((short) 2));
        Assert.assertEquals(UnsignedShort.MAX_VALUE.add(UnsignedShort.ONE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.add(UnsignedShort.MAX_VALUE), UnsignedShort.valueOf((short) -2));

        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).add(UnsignedShort.ZERO),
                UnsignedShort.valueOf("32768"));
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).add(UnsignedShort.ZERO),
                UnsignedShort.valueOf(Short.MAX_VALUE));
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).add(UnsignedShort.ONE),
                UnsignedShort.valueOf("32768"));
    }

    @Test(groups = { "unit" })
    public void testSubtract() {
        Assert.assertEquals(UnsignedShort.ZERO.subtract(null), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ZERO.subtract(UnsignedShort.MIN_VALUE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ONE.subtract(UnsignedShort.ONE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.valueOf((short) 2).subtract(UnsignedShort.ONE), UnsignedShort.ONE);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.subtract(UnsignedShort.ONE),
                UnsignedShort.valueOf("65534"));
        Assert.assertEquals(UnsignedShort.MAX_VALUE.subtract(UnsignedShort.MAX_VALUE), UnsignedShort.ZERO);

        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).subtract(UnsignedShort.ZERO),
                UnsignedShort.valueOf("32768"));
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).subtract(UnsignedShort.ZERO),
                UnsignedShort.valueOf(Short.MAX_VALUE));
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).subtract(UnsignedShort.ONE),
                UnsignedShort.valueOf((short) (Short.MAX_VALUE - 1)));
    }

    @Test(groups = { "unit" })
    public void testMultiply() {
        Assert.assertEquals(UnsignedShort.ONE.multiply(null), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ONE.multiply(UnsignedShort.MIN_VALUE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ONE.multiply(UnsignedShort.ONE), UnsignedShort.ONE);
        Assert.assertEquals(UnsignedShort.valueOf((short) 3).multiply(UnsignedShort.ONE),
                UnsignedShort.valueOf((short) 3));
        Assert.assertEquals(UnsignedShort.MAX_VALUE.multiply(UnsignedShort.ONE), UnsignedShort.MAX_VALUE);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.multiply(UnsignedShort.MAX_VALUE), UnsignedShort
                .valueOf((short) (65535 * 65535)));

        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).multiply(UnsignedShort.ZERO), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).multiply(UnsignedShort.ZERO), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).multiply(UnsignedShort.ONE),
                UnsignedShort.valueOf(Short.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testDivide() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedShort.ONE.divide(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedShort.ONE.divide(UnsignedShort.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedShort.ONE.divide(UnsignedShort.MIN_VALUE));

        Assert.assertEquals(UnsignedShort.ONE.divide(UnsignedShort.ONE), UnsignedShort.ONE);
        Assert.assertEquals(UnsignedShort.valueOf((short) 3).divide(UnsignedShort.ONE),
                UnsignedShort.valueOf((short) 3));
        Assert.assertEquals(UnsignedShort.ONE.divide(UnsignedShort.valueOf((short) 3)), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.divide(UnsignedShort.ONE), UnsignedShort.MAX_VALUE);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.divide(UnsignedShort.MAX_VALUE), UnsignedShort.ONE);

        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).divide(UnsignedShort.ONE),
                UnsignedShort.valueOf(Short.MIN_VALUE));
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).divide(UnsignedShort.ONE),
                UnsignedShort.valueOf(Short.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testRemainder() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedShort.ONE.remainder(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedShort.ONE.remainder(UnsignedShort.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedShort.ONE.remainder(UnsignedShort.MIN_VALUE));

        Assert.assertEquals(UnsignedShort.ONE.remainder(UnsignedShort.ONE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.valueOf((short) 3).remainder(UnsignedShort.ONE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.ONE.remainder(UnsignedShort.valueOf((short) 3)), UnsignedShort.ONE);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.remainder(UnsignedShort.ONE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.MAX_VALUE.remainder(UnsignedShort.MAX_VALUE), UnsignedShort.ZERO);

        Assert.assertEquals(UnsignedShort.valueOf(Short.MIN_VALUE).remainder(UnsignedShort.ONE), UnsignedShort.ZERO);
        Assert.assertEquals(UnsignedShort.valueOf(Short.MAX_VALUE).remainder(UnsignedShort.ONE), UnsignedShort.ZERO);
    }
}
