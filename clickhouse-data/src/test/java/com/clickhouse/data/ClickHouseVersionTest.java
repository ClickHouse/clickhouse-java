package com.clickhouse.data;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseVersionTest {
    private void check(ClickHouseVersion v, boolean latest, int year, int feature, int maintenance, int build) {
        Assert.assertNotNull(v);
        Assert.assertEquals(v.isLatest(), latest);
        Assert.assertEquals(v.getYear(), year);
        Assert.assertEquals(v.getMajorVersion(), year);
        Assert.assertEquals(v.getFeatureRelease(), feature);
        Assert.assertEquals(v.getMinorVersion(), feature);
        Assert.assertEquals(v.getMaintenanceRelease(), maintenance);
        Assert.assertEquals(v.getPatch(), maintenance);
        Assert.assertEquals(v.getBuilderNumber(), build);
    }

    @DataProvider(name = "versionProvider")
    private Object[][] getVersions() {
        // newVersion, oldVersion, indicator(0 - identitcal; 1 - same series; 2 -
        // greater)
        return new Object[][] { { "21.3.1.2345", "19", 2 }, { "21.3.1.2345", "21", 1 }, { "21.3.1.2345", "21.1", 2 },
                { "21.3.1.2345", "21.3", 1 }, { "21.3.12.2345", "21.3.2", 2 }, { "21.3.2.2345", "21.3.2.2345", 0 } };
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
    public void testStaticMethods() {
        Assert.assertTrue(ClickHouseVersion.compare("21.3.1", "21.3") > 0);
        Assert.assertTrue(ClickHouseVersion.check("21.3.1", "21.3"));
        Assert.assertFalse(ClickHouseVersion.check("21.3.1", "(,21.3)"));
        Assert.assertTrue(ClickHouseVersion.check("21.3.1", "(,21.3]"));
        Assert.assertTrue(ClickHouseVersion.check("21.3.1", "(21.3,)"));
    }

    @Test(groups = { "unit" })
    public void testParser() {
        check(ClickHouseVersion.parseVersion(null), false, 0, 0, 0, 0);
        check(ClickHouseVersion.parseVersion(""), false, 0, 0, 0, 0);
        check(ClickHouseVersion.parseVersion("twenty-one.three"), false, 0, 0, 0, 0);

        check(ClickHouseVersion.parseVersion(":21.3"), false, 21, 3, 0, 0);
        check(ClickHouseVersion.parseVersion(": 21.3"), false, 21, 3, 0, 0);
        check(ClickHouseVersion.parseVersion(":latest"), true, 0, 0, 0, 0);

        check(ClickHouseVersion.parseVersion("a1b2"), false, 0, 0, 0, 0);
        check(ClickHouseVersion.parseVersion("a1b 2abc"), false, 0, 0, 0, 0);
        check(ClickHouseVersion.parseVersion("a1.2.3.4"), false, 0, 0, 0, 0);
        check(ClickHouseVersion.parseVersion("a1b 2"), false, 2, 0, 0, 0);
        check(ClickHouseVersion.parseVersion("a1b 2 aaa"), false, 2, 0, 0, 0);
        check(ClickHouseVersion.parseVersion("1.2.3.4"), false, 1, 2, 3, 4);
        check(ClickHouseVersion.parseVersion("1.2.3.4.6"), false, 1, 2, 3, 4);
        check(ClickHouseVersion.parseVersion(" 1 . 2 . 3 . 4 . 6 "), false, 1, 2, 3, 4);
        check(ClickHouseVersion.parseVersion("upgrade from 021.03.00.01 to 21.7.8.9"), false, 21, 3, 0, 1);
        check(ClickHouseVersion.parseVersion("21.7..9 is supported"), false, 21, 7, 0, 0);

        check(ClickHouseVersion.parseVersion(
                "100000000000000000.10000000000000000000000000.100000000000000000000000000000.10000000000000000000000"),
                false, 0, 0, 0, 0);
    }

    @Test(groups = { "unit" })
    public void testCheckSingleVersion() {
        Assert.assertFalse(ClickHouseVersion.of(null).check(null));
        Assert.assertFalse(ClickHouseVersion.of(null).check(""));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.1").check(null));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.1").check(""));

        Assert.assertFalse(ClickHouseVersion.parseVersion("21").check("19"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").check("21.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").check("21.1.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").check("21.1.1.1"));

        Assert.assertFalse(ClickHouseVersion.parseVersion("21.1").check("19"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.1").check("21.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.1").check("21.1.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.1").check("21.1.2.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.1").check("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.1").check("21.0"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.1").check("21.1"));

        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1").check("19"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1").check("21.4"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1").check("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1").check("21.3.1.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1").check("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1").check("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1").check("21.3.1"));

        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.7").check("22"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.7").check("21.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.7").check("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.7").check("21.3.1.6"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.7").check("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.7").check("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.7").check("21.3.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.7").check("21.3.1.7"));
    }

    @Test(groups = { "unit" })
    public void testCheckRange() {
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("(,)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("(,]"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("[,)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("[,]"));

        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("(21.2,)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("(21.3,)"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.1").check("(,21.3)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("(,21.3]"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.1").check("(,21.2)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("[21.2,]"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.1.1").check("[21.3,]"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.1").check("[21.4,]"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.0.0").check("[,21.3]"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.1.1").check("[,21.2]"));

        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").check("(21.2,21.3)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").check("(21.2,21.3]"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").check("(21.3,21.4)"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").check("[21.3,21.4)"));

        Assert.assertTrue(ClickHouseVersion.parseVersion("21.8.8.29").check("[18.16,)"));

        Assert.assertFalse(ClickHouseVersion.of("").check("[18.16,)"));
        Assert.assertTrue(ClickHouseVersion.of("latest").check("[18.16,)"));

        Assert.assertTrue(ClickHouseVersion.of("latest").check("(,)"));
        Assert.assertTrue(ClickHouseVersion.of("latest").check("[,)"));
    }

    @Test(groups = { "unit" })
    public void testCheckMultipleRanges() {
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").check("(21.2,21.3],21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2").check("21.3,(21.2,21.3]"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").check("21.3,[21.2,21.3]"));
    }

    @Test(groups = { "unit" })
    public void testCheckWeirdRange() {
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").check(","));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").check(",,,,,,,,,, , , , , ,,,,,,,,,,"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").check("version: 21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.3.2")
                .check("preferred 21 series, up to 21.3, or 21.3.3.2 to be more specific"));
    }

    @Test(groups = { "unit" })
    public void testCompare() {
        Assert.assertTrue(
                ClickHouseVersion.parseVersion("1.1.12345").compareTo(ClickHouseVersion.parseVersion("21.3")) < 0);
        Assert.assertTrue(
                ClickHouseVersion.parseVersion("21.9").compareTo(ClickHouseVersion.parseVersion("19.16")) > 0);
        Assert.assertTrue(
                ClickHouseVersion.parseVersion("021.03").compareTo(ClickHouseVersion.parseVersion("21.3.0.0")) == 0);
        Assert.assertTrue(ClickHouseVersion.parseVersion(null).compareTo(ClickHouseVersion.parseVersion(" ")) == 0);
        Assert.assertTrue(
                ClickHouseVersion.parseVersion("21.3").compareTo(ClickHouseVersion.parseVersion("latest")) < 0);

        Assert.assertTrue(ClickHouseVersion.parseVersion(null).compareTo(null) == 0);
    }

    @Test(groups = { "unit" })
    public void testLatest() {
        Assert.assertTrue(ClickHouseVersion.parseVersion(":latest").isLatest());
        Assert.assertTrue(ClickHouseVersion.parseVersion("latest").isLatest());
        Assert.assertTrue(ClickHouseVersion.parseVersion(" Latest").isLatest());
        Assert.assertTrue(ClickHouseVersion.parseVersion("version: latest ").isLatest());
        Assert.assertFalse(ClickHouseVersion.parseVersion("latest version").isLatest());
    }

    @Test(dataProvider = "versionProvider", groups = { "unit" })
    public void testNewerVersion(String newVersion, String oldVersion, int same) {
        Assert.assertTrue(ClickHouseVersion.parseVersion(newVersion).isNewerOrEqualTo(oldVersion),
                newVersion + " should be newer than or equal to " + oldVersion);
        if (same <= 0) {
            Assert.assertFalse(ClickHouseVersion.parseVersion(newVersion).isNewerThan(oldVersion),
                    newVersion + " should NOT be newer than " + oldVersion);
        } else {
            Assert.assertTrue(ClickHouseVersion.parseVersion(newVersion).isNewerThan(oldVersion),
                    newVersion + " should be newer than " + oldVersion);
        }
    }

    @Test(dataProvider = "versionProvider", groups = { "unit" })
    public void testOlderVersion(String newVersion, String oldVersion, int same) {
        Assert.assertTrue(ClickHouseVersion.parseVersion(oldVersion).isOlderOrEqualTo(newVersion),
                oldVersion + " should be older than or euqal to " + newVersion);
        if (same >= 1) {
            Assert.assertTrue(ClickHouseVersion.parseVersion(oldVersion).isOlderThan(newVersion),
                    oldVersion + " should be older than " + newVersion);
        } else {
            Assert.assertFalse(ClickHouseVersion.parseVersion(oldVersion).isOlderThan(newVersion),
                    oldVersion + " should be older than " + newVersion);
        }
    }

    @Test(groups = { "unit" })
    public void testBelongsTo() {
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").belongsTo("latest"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("latest").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("latest").belongsTo("latest"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21").belongsTo("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").belongsTo("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2").belongsTo("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").belongsTo("21"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2").belongsTo("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").belongsTo("21.3"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").belongsTo("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").belongsTo("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2").belongsTo("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").belongsTo("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").belongsTo("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").belongsTo("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.2").belongsTo("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").belongsTo("21.3.2.1"));
    }

    @Test(groups = { "unit" })
    public void testIsBeyond() {
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").isNewerThan("latest"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("latest").isNewerThan("21.3"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("latest").isNewerThan("latest"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22.3").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22.3.2").isNewerThan("21"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22.3.2.1").isNewerThan("21"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").isNewerThan("21.3"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").isNewerThan("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2").isNewerThan("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").isNewerThan("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22").isNewerThan("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.4").isNewerThan("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.4.2").isNewerThan("21.3"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.4.2.1").isNewerThan("21.3"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").isNewerThan("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").isNewerThan("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.2").isNewerThan("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.1").isNewerThan("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22").isNewerThan("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.4").isNewerThan("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.3").isNewerThan("21.3.2"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.3.1").isNewerThan("21.3.2"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21").isNewerThan("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3").isNewerThan("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.2").isNewerThan("21.3.2.1"));
        Assert.assertFalse(ClickHouseVersion.parseVersion("21.3.2.1").isNewerThan("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("22").isNewerThan("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.4").isNewerThan("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.3").isNewerThan("21.3.2.1"));
        Assert.assertTrue(ClickHouseVersion.parseVersion("21.3.2.2").isNewerThan("21.3.2.1"));
    }
}
