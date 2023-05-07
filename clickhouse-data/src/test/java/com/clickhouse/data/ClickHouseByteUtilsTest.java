package com.clickhouse.data;

import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseByteUtilsTest {
    @DataProvider(name = "int16Provider")
    private Object[][] getInt16Args() {
        return new Object[][] {
                { new byte[2], 0 },
                { new byte[3], 1 },
                { new byte[] { 0, 1, 2 }, 0 },
                { new byte[] { 0, 1, 2 }, 1 },
                { new byte[] { -1, -2, -3 }, 0 },
                { new byte[] { -1, -2, -3 }, 1 },
        };
    }

    @DataProvider(name = "int32Provider")
    private Object[][] getInt32Args() {
        return new Object[][] {
                { new byte[4], 0 },
                { new byte[5], 1 },
                { new byte[] { 0, 1, 2, 3, 4 }, 0 },
                { new byte[] { 0, 1, 2, 3, 4 }, 1 },
                { new byte[] { -1, -2, -3, -4, -5 }, 0 },
                { new byte[] { -1, -2, -3, -4, -5 }, 1 },
        };
    }

    @DataProvider(name = "int64Provider")
    private Object[][] getInt64Args() {
        return new Object[][] {
                { new byte[8], 0 },
                { new byte[9], 1 },
                { new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, 0 },
                { new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, 1 },
                { new byte[] { -1, -2, -3, -4, -5, -6, -7, -8, -9 }, 0 },
                { new byte[] { -1, -2, -3, -4, -5, -6, -7, -8, -9 }, 1 },
        };
    }

    @Test(groups = { "unit" })
    public void testEquals() {
        Assert.assertTrue(ClickHouseByteUtils.equals(new byte[0], 0, 0, new byte[0], 0, 0));
        Assert.assertTrue(ClickHouseByteUtils.equals(new byte[] { 1, 2, 3 }, 0, 0, new byte[] { 1, 2, 3 }, 0, 0));
        Assert.assertTrue(ClickHouseByteUtils.equals(new byte[] { 1, 2, 3 }, 2, 2, new byte[] { 1, 2, 3 }, 2, 2));

        Assert.assertTrue(ClickHouseByteUtils.equals(new byte[] { 1, 2, 3 }, 0, 2, new byte[] { 1, 2, 3 }, 0, 2));
        Assert.assertTrue(ClickHouseByteUtils.equals(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 1, 2, 3 }, 1, 2));
        Assert.assertTrue(ClickHouseByteUtils.equals(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 0, 1, 2, 3 }, 2, 3));
    }

    @Test(groups = { "unit" })
    public void testGetOrCopy() {
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(null, -1), new byte[0]);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(ByteBuffer.allocate(0), -1), new byte[0]);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(ByteBuffer.allocate(1), -1), new byte[0]);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(ByteBuffer.allocate(1), 0), new byte[0]);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 'a', 'b', 'c', 'd', 'e' }, 0, 3);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(buffer, 1), new byte[] { 'a' });
        Assert.assertEquals(buffer.position(), 0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(buffer, 2), new byte[] { 'a', 'b' });
        Assert.assertEquals(buffer.position(), 0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(buffer, 3), new byte[] { 'a', 'b', 'c' });
        Assert.assertEquals(buffer.position(), 0);
        Assert.assertThrows(BufferUnderflowException.class, () -> ClickHouseByteUtils.getOrCopy(buffer, 4));
        Assert.assertEquals(buffer.position(), 0);

        ByteBuffer directBuf = ByteBuffer.allocateDirect(5);
        directBuf.put(new byte[] { 'a', 'b', 'c', 'd', 'e' });
        ((Buffer) directBuf).position(0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(directBuf, 1), new byte[] { 'a' });
        Assert.assertEquals(directBuf.position(), 0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(directBuf, 2), new byte[] { 'a', 'b' });
        Assert.assertEquals(directBuf.position(), 0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(directBuf, 3), new byte[] { 'a', 'b', 'c' });
        Assert.assertEquals(directBuf.position(), 0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(directBuf, 4), new byte[] { 'a', 'b', 'c', 'd' });
        Assert.assertEquals(directBuf.position(), 0);
        Assert.assertEquals(ClickHouseByteUtils.getOrCopy(directBuf, 5), new byte[] { 'a', 'b', 'c', 'd', 'e' });
        Assert.assertEquals(directBuf.position(), 0);
        Assert.assertThrows(BufferUnderflowException.class, () -> ClickHouseByteUtils.getOrCopy(directBuf, 6));
        Assert.assertEquals(directBuf.position(), 0);
        directBuf.clear();
    }

    @Test(groups = { "unit" })
    public void testIndexOf() {
        Assert.assertEquals(ClickHouseByteUtils.indexOf(null, null), -1);
        Assert.assertEquals(ClickHouseByteUtils.indexOf(null, new byte[0]), -1);
        Assert.assertEquals(ClickHouseByteUtils.indexOf(new byte[0], new byte[0]), -1);
        Assert.assertEquals(ClickHouseByteUtils.indexOf(new byte[] { 1 }, new byte[0]), 0);
        Assert.assertEquals(ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3 }, new byte[] { 2, 3 }), 1);

        Assert.assertEquals(ClickHouseByteUtils.indexOf(null, 3, 4, null, 1, 2), -1);
        Assert.assertEquals(ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3 }, 2, 1, new byte[] { 3 }, 0, 1), 2);
        Assert.assertEquals(ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 3, new byte[] { 3, 4 }, 1, 1),
                3);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 0, 5, new byte[] { 1, 2, 3, 4, 5 }, 1, 3), 1);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 3, new byte[] { 1, 4, 3, 2 }, 1, 2), -1);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 4, new byte[] { 1, 4, 5, 6, 3, 2 }, 1, 2),
                3);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 5 }, 1, 1, new byte[] { 1, 4, 5, 6, 3, 2 }, 2, 2),
                -1);

        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 5 }, 1, 1, new byte[] { 1, 4, 5, 6, 3, 2 }, 2, 2,
                        true),
                1);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 3, new byte[] { 1, 4, 5, 6, 3, 2 }, 1, 2,
                        true),
                3);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 4, new byte[] { 1, 4, 5, 6, 3, 2 }, 1, 2,
                        true),
                3);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 3, new byte[] { 1, 4, 5, 6, 3, 2 }, 2, 2,
                        true),
                -1);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 4, new byte[] { 1, 4, 5, 6, 3, 2 }, 2, 2,
                        true),
                4);
        Assert.assertEquals(
                ClickHouseByteUtils.indexOf(new byte[] { 1, 2, 3, 4, 5 }, 1, 4, new byte[] { 1, 4, 5, 6, 3, 2 }, 1, 5,
                        true),
                3);
    }

    @Test(dataProvider = "int16Provider", groups = { "unit" })
    public void testInt16(byte[] bytes, int offset) {
        byte[] arr = new byte[bytes.length];
        System.arraycopy(bytes, 0, arr, 0, bytes.length);
        Assert.assertEquals(arr, bytes);
        ClickHouseByteUtils.setInt16(arr, offset, ClickHouseByteUtils.getInt16(bytes, offset));
        Assert.assertEquals(arr, bytes);
    }

    @Test(dataProvider = "int32Provider", groups = { "unit" })
    public void testInt32(byte[] bytes, int offset) {
        byte[] arr = new byte[bytes.length];
        System.arraycopy(bytes, 0, arr, 0, bytes.length);
        Assert.assertEquals(arr, bytes);
        ClickHouseByteUtils.setInt32(arr, offset, ClickHouseByteUtils.getInt32(bytes, offset));
        Assert.assertEquals(arr, bytes);
    }

    @Test(dataProvider = "int64Provider", groups = { "unit" })
    public void testInt64(byte[] bytes, int offset) {
        byte[] arr = new byte[bytes.length];
        System.arraycopy(bytes, 0, arr, 0, bytes.length);
        Assert.assertEquals(arr, bytes);
        ClickHouseByteUtils.setInt64(arr, offset, ClickHouseByteUtils.getInt64(bytes, offset));
        Assert.assertEquals(arr, bytes);
    }

    @Test(dataProvider = "int32Provider", groups = { "unit" })
    public void testFloat32(byte[] bytes, int offset) {
        byte[] arr = new byte[bytes.length];
        System.arraycopy(bytes, 0, arr, 0, bytes.length);
        Assert.assertEquals(arr, bytes);
        ClickHouseByteUtils.setFloat32(arr, offset, ClickHouseByteUtils.getFloat32(bytes, offset));
        Assert.assertEquals(arr, bytes);
    }

    @Test(dataProvider = "int64Provider", groups = { "unit" })
    public void testFloat64(byte[] bytes, int offset) {
        byte[] arr = new byte[bytes.length];
        System.arraycopy(bytes, 0, arr, 0, bytes.length);
        Assert.assertEquals(arr, bytes);
        ClickHouseByteUtils.setFloat64(arr, offset, ClickHouseByteUtils.getFloat64(bytes, offset));
        Assert.assertEquals(arr, bytes);
    }
}
