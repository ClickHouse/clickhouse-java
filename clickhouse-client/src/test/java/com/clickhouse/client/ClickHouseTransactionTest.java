package com.clickhouse.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.clickhouse.client.ClickHouseTransaction.XID;
import com.clickhouse.data.value.UnsignedLong;

@SuppressWarnings("deprecation")
public class ClickHouseTransactionTest {
    @DataProvider(name = "validTuples")
    public Object[][] getValidTuples() {
        return new Object[][] {
                { Arrays.asList(UnsignedLong.valueOf(10L), UnsignedLong.valueOf(20L), "host-uuid"), 10L, 20L,
                        "host-uuid", null, "(10,20,'host-uuid')" },
                { Arrays.asList(15L, 25L, UUID.fromString("00000000-0000-0000-0000-000000000001"), 5L), 15L, 25L,
                        "00000000-0000-0000-0000-000000000001", 5L,
                        "(15,25,'00000000-0000-0000-0000-000000000001',5)" },
                { Arrays.asList(UnsignedLong.valueOf(100L), UnsignedLong.valueOf(200L), "host-4", 0L), 100L, 200L,
                        "host-4", 0L, "(100,200,'host-4',0)" },
                { Arrays.asList(1L, 2L, "host-null-version", null), 1L, 2L, "host-null-version", null,
                        "(1,2,'host-null-version')" },
        };
    }

    @Test(groups = "unit", dataProvider = "validTuples")
    public void testParseValidXid(List<?> list, long expectedSnapshot, long expectedCounter, String expectedHost,
            Long expectedSessionVersion, String expectedTupleString) {
        XID xid = XID.of(list);
        Assert.assertEquals(xid.getSnapshotVersion(), expectedSnapshot);
        Assert.assertEquals(xid.getLocalTransactionCounter(), expectedCounter);
        Assert.assertEquals(xid.getHostId(), expectedHost);
        if (expectedSessionVersion != null) {
            Assert.assertTrue(xid.getSessionNodeVersion().isPresent());
            Assert.assertEquals(xid.getSessionNodeVersion().get(), expectedSessionVersion);
        } else {
            Assert.assertFalse(xid.getSessionNodeVersion().isPresent());
        }
        Assert.assertEquals(xid.asTupleString(), expectedTupleString);
    }

    @DataProvider(name = "emptyTuples")
    public Object[][] getEmptyTuples() {
        return new Object[][] {
                { Arrays.asList(UnsignedLong.valueOf(0L), UnsignedLong.valueOf(0L),
                        "00000000-0000-0000-0000-000000000000") },
                { Arrays.asList(0L, 0L, UUID.fromString("00000000-0000-0000-0000-000000000000"), 0L) },
                { Arrays.asList(0L, 0L, "00000000-0000-0000-0000-000000000000", 1L) },
        };
    }

    @Test(groups = "unit", dataProvider = "emptyTuples")
    public void testParseEmptyXid(List<?> list) {
        XID xid = XID.of(list);
        Assert.assertSame(xid, XID.EMPTY);
        Assert.assertEquals(xid, XID.EMPTY);
    }

    @DataProvider(name = "invalidTuples")
    public Object[][] getInvalidTuples() {
        return new Object[][] {
                { null },
                { Collections.emptyList() },
                { Collections.singletonList(1L) },
                { Arrays.asList(1L, 2L) },
                { Arrays.asList(1L, 2L, "host", 0L, "extra") },
        };
    }

    @Test(groups = "unit", dataProvider = "invalidTuples", expectedExceptions = IllegalArgumentException.class)
    public void testParseInvalidXid(List<?> list) {
        XID.of(list);
    }

    @Test(groups = "unit")
    public void testEqualsAndHashCode() {
        XID xid3a = XID.of(Arrays.asList(1L, 2L, "host1"));
        XID xid3b = XID.of(Arrays.asList(1L, 2L, "host1"));
        XID xid3c = XID.of(Arrays.asList(1L, 3L, "host1"));

        XID xid4a = XID.of(Arrays.asList(1L, 2L, "host1", 10L));
        XID xid4b = XID.of(Arrays.asList(1L, 2L, "host1", 10L));
        XID xid4c = XID.of(Arrays.asList(1L, 2L, "host1", 20L));

        Assert.assertEquals(xid3a, xid3a);
        Assert.assertEquals(xid3a, xid3b);
        Assert.assertEquals(xid3a.hashCode(), xid3b.hashCode());
        Assert.assertNotEquals(xid3a, xid3c);

        Assert.assertEquals(xid4a, xid4a);
        Assert.assertEquals(xid4a, xid4b);
        Assert.assertEquals(xid4a.hashCode(), xid4b.hashCode());
        Assert.assertNotEquals(xid4a, xid4c);

        Assert.assertNotEquals(xid3a, xid4a);
        Assert.assertNotEquals(xid4a, xid3a);
        Assert.assertNotEquals(xid4a, null);
        Assert.assertNotEquals(xid4a, "(1,2,'host1',10)");
    }

    @Test(groups = "unit")
    public void testToString() {
        XID xid3 = XID.of(Arrays.asList(1L, 2L, "host1"));
        Assert.assertTrue(xid3.toString().contains("snapshotVersion=1"));
        Assert.assertTrue(xid3.toString().contains("localTxCounter=2"));
        Assert.assertTrue(xid3.toString().contains("hostId=host1"));
        Assert.assertFalse(xid3.toString().contains("sessionNodeVersion="));

        XID xid4 = XID.of(Arrays.asList(1L, 2L, "host1", 5L));
        Assert.assertTrue(xid4.toString().contains("snapshotVersion=1"));
        Assert.assertTrue(xid4.toString().contains("localTxCounter=2"));
        Assert.assertTrue(xid4.toString().contains("hostId=host1"));
        Assert.assertTrue(xid4.toString().contains("sessionNodeVersion=5"));
    }
}
