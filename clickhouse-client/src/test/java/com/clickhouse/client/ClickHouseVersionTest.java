package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseVersionTest {
    private void check(ClickHouseVersion v, boolean latest, int year, int major, int minor, int internal) {
        Assert.assertNotNull(v);
        Assert.assertEquals(v.isLatest(), latest);
        Assert.assertEquals(v.getYear(), year);
        Assert.assertEquals(v.getMajor(), major);
        Assert.assertEquals(v.getMinor(), minor);
        Assert.assertEquals(v.getInternal(), internal);
    }

    @DataProvider(name = "versionProvider")
    private Object[][] getVersions() {
        // newVersion, oldVersion, sameOrNot
        return new Object[][] { { "21.3.1.2345", "21.0", false }, { "21.3.1.2345", "21.3", false },
                { "21.3.12.2345", "21.3.2", false }, { "21.3.2.2345", "21.3.2.2345", true } };
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        check(new ClickHouseVersion(false, -1, -2, -3, -4), false, 0, 0, 0, 0);
        check(new ClickHouseVersion(true, -1, -2, -3, -4), true, 0, 0, 0, 0);
        check(new ClickHouseVersion(false, 0, 0, 0, 0), false, 0, 0, 0, 0);
        check(new ClickHouseVersion(true, 0, 0, 0, 0), true, 0, 0, 0, 0);
        check(new ClickHouseVersion(false, 5, 4, 3, 2), false, 5, 4, 3, 2);
        check(new ClickHouseVersion(true, 5, 4, 3, 2), true, 0, 0, 0, 0);
    }

    @Test(groups = { "unit" })
    public void testParser() {
        check(ClickHouseVersion.of(null), false, 0, 0, 0, 0);
        check(ClickHouseVersion.of(""), false, 0, 0, 0, 0);
        check(ClickHouseVersion.of("twenty-one.three"), false, 0, 0, 0, 0);

        check(ClickHouseVersion.of(":21.3"), false, 21, 3, 0, 0);
        check(ClickHouseVersion.of(": 21.3"), false, 21, 3, 0, 0);
        check(ClickHouseVersion.of(":latest"), true, 0, 0, 0, 0);

        check(ClickHouseVersion.of("a1b2"), false, 0, 0, 0, 0);
        check(ClickHouseVersion.of("a1b 2abc"), false, 0, 0, 0, 0);
        check(ClickHouseVersion.of("a1.2.3.4"), false, 0, 0, 0, 0);
        check(ClickHouseVersion.of("a1b 2"), false, 2, 0, 0, 0);
        check(ClickHouseVersion.of("a1b 2 aaa"), false, 2, 0, 0, 0);
        check(ClickHouseVersion.of("1.2.3.4"), false, 1, 2, 3, 4);
        check(ClickHouseVersion.of("1.2.3.4.6"), false, 1, 2, 3, 4);
        check(ClickHouseVersion.of(" 1 . 2 . 3 . 4 . 6 "), false, 1, 2, 3, 4);
        check(ClickHouseVersion.of("upgrade from 021.03.00.01 to 21.7.8.9"), false, 21, 3, 0, 1);
        check(ClickHouseVersion.of("21.7..9 is supported"), false, 21, 7, 0, 0);

        check(ClickHouseVersion.of(
                "100000000000000000.10000000000000000000000000.100000000000000000000000000000.10000000000000000000000"),
                false, 0, 0, 0, 0);
    }

    @Test(groups = { "unit" })
    public void testCompare() {
        Assert.assertTrue(ClickHouseVersion.of("1.1.12345").compareTo(ClickHouseVersion.of("21.3")) < 0);
        Assert.assertTrue(ClickHouseVersion.of("21.9").compareTo(ClickHouseVersion.of("19.16")) > 0);
        Assert.assertTrue(ClickHouseVersion.of("021.03").compareTo(ClickHouseVersion.of("21.3.0.0")) == 0);
        Assert.assertTrue(ClickHouseVersion.of(null).compareTo(ClickHouseVersion.of(" ")) == 0);
        Assert.assertTrue(ClickHouseVersion.of("21.3").compareTo(ClickHouseVersion.of("latest")) < 0);

        Assert.assertThrows(NullPointerException.class, () -> ClickHouseVersion.of(null).compareTo(null));
    }

    @Test(groups = { "unit" })
    public void testLatest() {
        Assert.assertTrue(ClickHouseVersion.of(":latest").isLatest());
        Assert.assertTrue(ClickHouseVersion.of("latest").isLatest());
        Assert.assertTrue(ClickHouseVersion.of(" Latest").isLatest());
        Assert.assertTrue(ClickHouseVersion.of("version: latest ").isLatest());
        Assert.assertFalse(ClickHouseVersion.of("latest version").isLatest());
    }

    @Test(dataProvider = "versionProvider", groups = { "unit" })
    public void testNewerVersion(String newVersion, String oldVersion, boolean same) {
        Assert.assertTrue(ClickHouseVersion.of(newVersion).isNewerOrEqualTo(oldVersion),
                newVersion + " should be newer than or equal to " + oldVersion);
        if (same) {
            Assert.assertFalse(ClickHouseVersion.of(newVersion).isNewerThan(oldVersion),
                    newVersion + " should NOT be newer than " + oldVersion);
        } else {
            Assert.assertTrue(ClickHouseVersion.of(newVersion).isNewerThan(oldVersion),
                    newVersion + " should be newer than " + oldVersion);
        }
    }

    @Test(dataProvider = "versionProvider", groups = { "unit" })
    public void testOlderVersion(String newVersion, String oldVersion, boolean same) {
        Assert.assertTrue(ClickHouseVersion.of(oldVersion).isOlderOrEqualTo(newVersion),
                oldVersion + " should be older than or euqal to " + newVersion);
        if (same) {
            Assert.assertFalse(ClickHouseVersion.of(oldVersion).isOlderThan(newVersion),
                    oldVersion + " should NOT be older than " + newVersion);
        } else {
            Assert.assertTrue(ClickHouseVersion.of(oldVersion).isOlderThan(newVersion),
                    oldVersion + " should be older than " + newVersion);
        }
    }

    @Test(groups = { "unit" })
    public void testBelongsTo() {
        Assert.assertFalse(ClickHouseVersion.of("21.3").belongsTo("latest"));
        Assert.assertFalse(ClickHouseVersion.of("latest").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("latest").belongsTo("latest"));
        Assert.assertTrue(ClickHouseVersion.of("21").belongsTo("21"));
        Assert.assertTrue(ClickHouseVersion.of("21.3").belongsTo("21"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2").belongsTo("21"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2.1").belongsTo("21"));
        Assert.assertFalse(ClickHouseVersion.of("21").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("21.3").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2.1").belongsTo("21.3"));
        Assert.assertFalse(ClickHouseVersion.of("21").belongsTo("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.of("21.3").belongsTo("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2").belongsTo("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2.1").belongsTo("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.of("21").belongsTo("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.of("21.3").belongsTo("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2").belongsTo("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2.1").belongsTo("21.3.2.1"));
    }

    @Test(groups = { "unit" })
    public void testIsBeyond() {
        Assert.assertFalse(ClickHouseVersion.of("21.3").isBeyond("latest"));
        Assert.assertTrue(ClickHouseVersion.of("latest").isBeyond("21.3"));
        Assert.assertFalse(ClickHouseVersion.of("latest").isBeyond("latest"));
        Assert.assertFalse(ClickHouseVersion.of("21").isBeyond("21"));
        Assert.assertFalse(ClickHouseVersion.of("21.3").isBeyond("21"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2").isBeyond("21"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2.1").isBeyond("21"));
        Assert.assertTrue(ClickHouseVersion.of("22").isBeyond("21"));
        Assert.assertTrue(ClickHouseVersion.of("22.3").isBeyond("21"));
        Assert.assertTrue(ClickHouseVersion.of("22.3.2").isBeyond("21"));
        Assert.assertTrue(ClickHouseVersion.of("22.3.2.1").isBeyond("21"));
        Assert.assertFalse(ClickHouseVersion.of("21").isBeyond("21.3"));
        Assert.assertFalse(ClickHouseVersion.of("21.3").isBeyond("21.3"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2").isBeyond("21.3"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2.1").isBeyond("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("22").isBeyond("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("21.4").isBeyond("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("21.4.2").isBeyond("21.3"));
        Assert.assertTrue(ClickHouseVersion.of("21.4.2.1").isBeyond("21.3"));
        Assert.assertFalse(ClickHouseVersion.of("21").isBeyond("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.of("21.3").isBeyond("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2").isBeyond("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2.1").isBeyond("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.of("22").isBeyond("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.of("21.4").isBeyond("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.3").isBeyond("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.3.1").isBeyond("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.of("21").isBeyond("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.of("21.3").isBeyond("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2").isBeyond("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.of("21.3.2.1").isBeyond("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.of("22").isBeyond("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.of("21.4").isBeyond("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.3").isBeyond("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.of("21.3.2.2").isBeyond("21.3.2.1"));
    }
}
