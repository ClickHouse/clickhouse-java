package com.clickhouse.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseByteBufferTest {
    @Test(groups = { "unit" })
    public void testEmptyArray() {
        Assert.assertEquals(ClickHouseByteBuffer.of(null), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(null, -1, -1), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[0]), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[0], -1, -1), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 0, 0), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, -1, 0), ClickHouseByteBuffer.newInstance());

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update(null),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update(null, -1, -1),
                ClickHouseByteBuffer.newInstance());
    }

    @Test(groups = { "unit" })
    public void testInvalidValue() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, -1, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 0, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 3, 1));
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        ClickHouseByteBuffer buf1 = ClickHouseByteBuffer.newInstance();
        Assert.assertEquals(buf1.array(), ClickHouseByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(buf1.position(), 0);
        Assert.assertEquals(buf1.length(), 0);
        Assert.assertEquals(buf1.limit(), 0);

        ClickHouseByteBuffer buf2 = ClickHouseByteBuffer.newInstance();
        Assert.assertEquals(buf1.array(), ClickHouseByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(buf1.position(), 0);
        Assert.assertEquals(buf1.length(), 0);
        Assert.assertEquals(buf1.limit(), 0);

        Assert.assertFalse(buf1 == buf2, "Should be different instances");
        Assert.assertEquals(buf1, buf2);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2).reset(),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().update(new byte[] { 1, 2, 3 }, 1, 2),
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2));
    }
}
