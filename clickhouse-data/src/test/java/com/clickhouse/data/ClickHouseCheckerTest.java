package com.clickhouse.data;

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseCheckerTest {
    @Test(groups = { "unit" })
    public void testBetween() {
        // int
        Assert.assertEquals(ClickHouseChecker.between(0, "value", 0, 0), 0);
        Assert.assertEquals(ClickHouseChecker.between(0, "value", -1, 1), 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.between(1, "value", 2, 3));

        // long
        Assert.assertEquals(ClickHouseChecker.between(0L, "value", 0L, 0L), 0L);
        Assert.assertEquals(ClickHouseChecker.between(0L, "value", -1L, 1L), 0L);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.between(1L, "value", 2L, 3L));

        // bigint
        Assert.assertEquals(ClickHouseChecker.between(BigInteger.ZERO, "value", BigInteger.ZERO, BigInteger.ZERO),
                BigInteger.ZERO);
        Assert.assertEquals(
                ClickHouseChecker.between(BigInteger.ZERO, "value", BigInteger.valueOf(-1L), BigInteger.ONE),
                BigInteger.ZERO);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.between(BigInteger.ONE, "value",
                BigInteger.valueOf(2), BigInteger.valueOf(3L)));
    }

    @Test(groups = { "unit" })
    public void testIsNullOrEmpty() {
        Assert.assertTrue(ClickHouseChecker.isNullOrEmpty(null));
        Assert.assertTrue(ClickHouseChecker.isNullOrEmpty(new StringBuilder()));
        Assert.assertTrue(ClickHouseChecker.isNullOrEmpty(new StringBuffer()));
        Assert.assertTrue(ClickHouseChecker.isNullOrEmpty(""));
        Assert.assertFalse(ClickHouseChecker.isNullOrEmpty(" "));
    }

    @Test(groups = { "unit" })
    public void testIsNullOrBlank() {
        Assert.assertTrue(ClickHouseChecker.isNullOrBlank(null));
        Assert.assertTrue(ClickHouseChecker.isNullOrEmpty(new StringBuilder()));
        Assert.assertTrue(ClickHouseChecker.isNullOrEmpty(new StringBuffer()));
        Assert.assertTrue(ClickHouseChecker.isNullOrBlank(""));
        Assert.assertTrue(ClickHouseChecker.isNullOrBlank(" \t\r\n  "));
    }

    @Test(groups = { "unit" })
    public void testNonBlank() {
        Assert.assertEquals(ClickHouseChecker.nonBlank(" 1", "value"), " 1");

        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.nonBlank(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.nonBlank("", ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.nonBlank(" ", ""));
    }

    @Test(groups = { "unit" })
    public void testNonEmpty() {
        Assert.assertEquals(ClickHouseChecker.nonEmpty(" ", "value"), " ");

        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.nonEmpty(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.nonEmpty("", ""));
    }

    @Test(groups = { "unit" })
    public void testNonNull() {
        Object obj;
        Assert.assertEquals(ClickHouseChecker.nonNull(obj = new Object(), "value"), obj);
        Assert.assertEquals(ClickHouseChecker.nonNull(obj = 1, "value"), obj);

        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.nonNull(null, null));
    }

    @Test(groups = { "unit" })
    public void testNotLessThan() {
        // int
        Assert.assertEquals(ClickHouseChecker.notLessThan(1, "value", 0), 1);
        Assert.assertEquals(ClickHouseChecker.notLessThan(0, "value", 0), 0);
        Assert.assertEquals(ClickHouseChecker.notLessThan(0, "value", -1), 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.notLessThan(0, "value", 1));

        // long
        Assert.assertEquals(ClickHouseChecker.notLessThan(1L, "value", 0L), 1L);
        Assert.assertEquals(ClickHouseChecker.notLessThan(0L, "value", 0L), 0L);
        Assert.assertEquals(ClickHouseChecker.notLessThan(0L, "value", -1L), 0L);
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.notLessThan(0L, "value", 1L));

        // bigint
        Assert.assertEquals(ClickHouseChecker.notLessThan(BigInteger.ONE, "value", BigInteger.ZERO), BigInteger.ONE);
        Assert.assertEquals(ClickHouseChecker.notLessThan(BigInteger.ZERO, "value", BigInteger.ZERO), BigInteger.ZERO);
        Assert.assertEquals(ClickHouseChecker.notLessThan(BigInteger.ZERO, "value", BigInteger.valueOf(-1L)),
                BigInteger.ZERO);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseChecker.notLessThan(BigInteger.ZERO, "value", BigInteger.ONE));
    }

    @Test(groups = { "unit" })
    public void testNotLongerThan() {
        byte[] bytes;
        Assert.assertEquals(ClickHouseChecker.notLongerThan(bytes = null, "value", 0), bytes);
        Assert.assertEquals(ClickHouseChecker.notLongerThan(bytes = new byte[0], "value", 0), bytes);
        Assert.assertEquals(ClickHouseChecker.notLongerThan(bytes = new byte[1], "value", 1), bytes);

        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseChecker.notLongerThan(null, null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseChecker.notLongerThan(new byte[0], null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseChecker.notLongerThan(new byte[2], null, 1));
    }
}
