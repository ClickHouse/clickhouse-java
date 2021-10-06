package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseVersionTest {
    private void check(ClickHouseVersion v, int year, int major, int minor, int internal) {
        Assert.assertNotNull(v);
        Assert.assertEquals(v.getYear(), year);
        Assert.assertEquals(v.getMajor(), major);
        Assert.assertEquals(v.getMinor(), minor);
        Assert.assertEquals(v.getInternal(), internal);
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        check(new ClickHouseVersion(-1, -2, -3, -4), 0, 0, 0, 0);
        check(new ClickHouseVersion(0, 0, 0, 0), 0, 0, 0, 0);
        check(new ClickHouseVersion(5, 4, 3, 2), 5, 4, 3, 2);
    }

    @Test(groups = { "unit" })
    public void testParser() {
        check(ClickHouseVersion.of(null), 0, 0, 0, 0);
        check(ClickHouseVersion.of(""), 0, 0, 0, 0);
        check(ClickHouseVersion.of("twenty-one.three"), 0, 0, 0, 0);

        check(ClickHouseVersion.of("a1b2"), 0, 0, 0, 0);
        check(ClickHouseVersion.of("a1b 2abc"), 0, 0, 0, 0);
        check(ClickHouseVersion.of("a1.2.3.4"), 0, 0, 0, 0);
        check(ClickHouseVersion.of("a1b 2"), 2, 0, 0, 0);
        check(ClickHouseVersion.of("a1b 2 aaa"), 2, 0, 0, 0);
        check(ClickHouseVersion.of("1.2.3.4"), 1, 2, 3, 4);
        check(ClickHouseVersion.of("1.2.3.4.6"), 1, 2, 3, 4);
        check(ClickHouseVersion.of(" 1 . 2 . 3 . 4 . 6 "), 1, 2, 3, 4);
        check(ClickHouseVersion.of("upgrade from 021.03.00.01 to 21.7.8.9"), 21, 3, 0, 1);
        check(ClickHouseVersion.of("21.7..9 is supported"), 21, 7, 0, 0);

        check(ClickHouseVersion.of(
                "100000000000000000.10000000000000000000000000.100000000000000000000000000000.10000000000000000000000"),
                0, 0, 0, 0);
    }

    @Test(groups = { "unit" })
    public void testCompare() {
        Assert.assertTrue(ClickHouseVersion.of("1.1.12345").compareTo(ClickHouseVersion.of("21.3")) < 0);
        Assert.assertTrue(ClickHouseVersion.of("21.9").compareTo(ClickHouseVersion.of("19.16")) > 0);
        Assert.assertTrue(ClickHouseVersion.of("021.03").compareTo(ClickHouseVersion.of("21.3.0.0")) == 0);
        Assert.assertTrue(ClickHouseVersion.of(null).compareTo(ClickHouseVersion.of(" ")) == 0);

        Assert.assertThrows(NullPointerException.class, () -> ClickHouseVersion.of(null).compareTo(null));
    }
}
