package com.clickhouse.data.value;

import org.testng.Assert;
import org.testng.annotations.Test;

public class UnsignedByteTest {
    @Test(groups = { "unit" })
    public void testConversion() {
        Assert.assertEquals(UnsignedByte.ZERO.byteValue(), (byte) 0);
        Assert.assertEquals(UnsignedByte.ZERO.shortValue(), (short) 0);
        Assert.assertEquals(UnsignedByte.ZERO.intValue(), 0);
        Assert.assertEquals(UnsignedByte.ZERO.longValue(), 0L);
        Assert.assertEquals(UnsignedByte.ZERO.floatValue(), 0F);
        Assert.assertEquals(UnsignedByte.ZERO.doubleValue(), 0D);
        Assert.assertEquals(UnsignedByte.ZERO.toString(), "0");

        Assert.assertEquals(UnsignedByte.ONE.byteValue(), (byte) 1);
        Assert.assertEquals(UnsignedByte.ONE.shortValue(), (short) 1);
        Assert.assertEquals(UnsignedByte.ONE.intValue(), 1);
        Assert.assertEquals(UnsignedByte.ONE.longValue(), 1L);
        Assert.assertEquals(UnsignedByte.ONE.floatValue(), 1F);
        Assert.assertEquals(UnsignedByte.ONE.doubleValue(), 1D);
        Assert.assertEquals(UnsignedByte.ONE.toString(), "1");

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).byteValue(), (byte) -128);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).shortValue(), (short) 128);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).intValue(), 128);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).longValue(), 128L);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).floatValue(), 128F);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).doubleValue(), 128D);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).toString(), "128");

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).byteValue(), (byte) 127);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).shortValue(), (short) 127);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).intValue(), 127);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).longValue(), 127L);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).floatValue(), 127F);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).doubleValue(), 127D);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).toString(), "127");

        Assert.assertEquals(UnsignedByte.MAX_VALUE.byteValue(), (byte) 255);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.shortValue(), (short) 255);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.intValue(), 255);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.longValue(), 255L);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.floatValue(), 255F);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.doubleValue(), 255D);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.toString(), "255");
    }

    @Test(groups = { "unit" })
    public void testValueOf() {
        Assert.assertTrue(UnsignedByte.ZERO == UnsignedByte.MIN_VALUE);
        for (int i = 0; i <= 255; i++) {
            Assert.assertTrue(UnsignedByte.valueOf((byte) i) == UnsignedByte.valueOf(String.valueOf(i)));
            Assert.assertEquals(UnsignedByte.valueOf((byte) i).byteValue(), (byte) i);
            Assert.assertEquals(UnsignedByte.valueOf((byte) i).intValue(), i);
            Assert.assertEquals(UnsignedByte.valueOf((byte) i).toString(), String.valueOf(i));
        }

        Assert.assertThrows(NumberFormatException.class, () -> UnsignedByte.valueOf((String) null));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedByte.valueOf(""));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedByte.valueOf("-1"));
        Assert.assertThrows(NumberFormatException.class, () -> UnsignedByte.valueOf("256"));
    }

    @Test(groups = { "unit" })
    public void testAdd() {
        Assert.assertEquals(UnsignedByte.ZERO.add(null), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ZERO.add(UnsignedByte.MIN_VALUE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ONE.add(UnsignedByte.ONE), UnsignedByte.valueOf((byte) 2));
        Assert.assertEquals(UnsignedByte.MAX_VALUE.add(UnsignedByte.ONE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.add(UnsignedByte.MAX_VALUE), UnsignedByte.valueOf((byte) -2));

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).add(UnsignedByte.ZERO),
                UnsignedByte.valueOf("128"));
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).add(UnsignedByte.ZERO),
                UnsignedByte.valueOf(Byte.MAX_VALUE));
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).add(UnsignedByte.ONE), UnsignedByte.valueOf("128"));
    }

    @Test(groups = { "unit" })
    public void testSubtract() {
        Assert.assertEquals(UnsignedByte.ZERO.subtract(null), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ZERO.subtract(UnsignedByte.MIN_VALUE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ONE.subtract(UnsignedByte.ONE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.valueOf((byte) 2).subtract(UnsignedByte.ONE), UnsignedByte.ONE);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.subtract(UnsignedByte.ONE),
                UnsignedByte.valueOf("254"));
        Assert.assertEquals(UnsignedByte.MAX_VALUE.subtract(UnsignedByte.MAX_VALUE), UnsignedByte.ZERO);

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).subtract(UnsignedByte.ZERO),
                UnsignedByte.valueOf("128"));
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).subtract(UnsignedByte.ZERO),
                UnsignedByte.valueOf(Byte.MAX_VALUE));
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).subtract(UnsignedByte.ONE),
                UnsignedByte.valueOf((byte) (Byte.MAX_VALUE - 1)));
    }

    @Test(groups = { "unit" })
    public void testMultiply() {
        Assert.assertEquals(UnsignedByte.ONE.multiply(null), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ONE.multiply(UnsignedByte.MIN_VALUE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ONE.multiply(UnsignedByte.ONE), UnsignedByte.ONE);
        Assert.assertEquals(UnsignedByte.valueOf((byte) 3).multiply(UnsignedByte.ONE), UnsignedByte.valueOf((byte) 3));
        Assert.assertEquals(UnsignedByte.MAX_VALUE.multiply(UnsignedByte.ONE), UnsignedByte.MAX_VALUE);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.multiply(UnsignedByte.MAX_VALUE), UnsignedByte
                .valueOf((byte) (255 * 255)));

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).multiply(UnsignedByte.ZERO), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).multiply(UnsignedByte.ZERO), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).multiply(UnsignedByte.ONE),
                UnsignedByte.valueOf(Byte.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testDivide() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedByte.ONE.divide(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedByte.ONE.divide(UnsignedByte.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedByte.ONE.divide(UnsignedByte.MIN_VALUE));

        Assert.assertEquals(UnsignedByte.ONE.divide(UnsignedByte.ONE), UnsignedByte.ONE);
        Assert.assertEquals(UnsignedByte.valueOf((byte) 3).divide(UnsignedByte.ONE), UnsignedByte.valueOf((byte) 3));
        Assert.assertEquals(UnsignedByte.ONE.divide(UnsignedByte.valueOf((byte) 3)), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.divide(UnsignedByte.ONE), UnsignedByte.MAX_VALUE);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.divide(UnsignedByte.MAX_VALUE), UnsignedByte.ONE);

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).divide(UnsignedByte.ONE),
                UnsignedByte.valueOf(Byte.MIN_VALUE));
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).divide(UnsignedByte.ONE),
                UnsignedByte.valueOf(Byte.MAX_VALUE));
    }

    @Test(groups = { "unit" })
    public void testRemainder() {
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedByte.ONE.remainder(null));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedByte.ONE.remainder(UnsignedByte.ZERO));
        Assert.assertThrows(ArithmeticException.class, () -> UnsignedByte.ONE.remainder(UnsignedByte.MIN_VALUE));

        Assert.assertEquals(UnsignedByte.ONE.remainder(UnsignedByte.ONE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.valueOf((byte) 3).remainder(UnsignedByte.ONE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.ONE.remainder(UnsignedByte.valueOf((byte) 3)), UnsignedByte.ONE);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.remainder(UnsignedByte.ONE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.MAX_VALUE.remainder(UnsignedByte.MAX_VALUE), UnsignedByte.ZERO);

        Assert.assertEquals(UnsignedByte.valueOf(Byte.MIN_VALUE).remainder(UnsignedByte.ONE), UnsignedByte.ZERO);
        Assert.assertEquals(UnsignedByte.valueOf(Byte.MAX_VALUE).remainder(UnsignedByte.ONE), UnsignedByte.ZERO);
    }
}
