package com.clickhouse.data;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseByteBufferTest {
    @Test(groups = { "unit" })
    public void testEmptyArray() {
        Assert.assertEquals(ClickHouseByteBuffer.of((byte[]) null), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of((byte[]) null, -1, -1), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of((List<byte[]>) null, -1, -1),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[0]), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[0], -1, -1), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 0, 0),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, -1, 0),
                ClickHouseByteBuffer.newInstance());

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update((ByteBuffer) null),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update(ByteBuffer.allocate(0)),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 })
                        .update((ByteBuffer) ((Buffer) ByteBuffer.allocate(1)).limit(0)),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update(ByteBuffer.allocateDirect(0)),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 })
                        .update((ByteBuffer) ((Buffer) ByteBuffer.allocateDirect(1)).limit(0)),
                ClickHouseByteBuffer.newInstance());

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update((byte[]) null),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update((byte[]) null, -1, -1),
                ClickHouseByteBuffer.newInstance());

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update((List<byte[]>) null, -1, -1),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }).update(Collections.emptyList(), -1, -1),
                ClickHouseByteBuffer.newInstance());
    }

    @Test(groups = { "unit" })
    public void testCopy() {
        ClickHouseByteBuffer buf = ClickHouseByteBuffer.newInstance();
        Assert.assertFalse(buf == buf.copy(false));
        Assert.assertFalse(buf == buf.copy(true));
        Assert.assertEquals(buf.copy(false), buf);
        Assert.assertEquals(buf.copy(true), buf);

        buf = ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5, 6 });
        Assert.assertFalse(buf == buf.copy(false));
        Assert.assertFalse(buf == buf.copy(true));
        Assert.assertEquals(buf.copy(false), buf);
        Assert.assertEquals(buf.copy(true), buf);

        buf = ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5, 6 }, 3, 2);
        Assert.assertFalse(buf == buf.copy(false));
        Assert.assertFalse(buf == buf.copy(true));
        Assert.assertEquals(buf.copy(false), buf);
        Assert.assertNotEquals(buf.copy(true), buf);
    }

    @Test(groups = { "unit" }, enabled = false)
    public void testInvalidValue() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, -1, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 0, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 3, 1));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(Arrays.asList(new byte[0], null, new byte[0]), -1, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(Arrays.asList(new byte[] { 1, 2, 3 }), -1, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(Arrays.asList(new byte[] { 1, 2, 3 }), 0, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseByteBuffer.of(Arrays.asList(new byte[] { 1, 2, 3 }), -1, -1));
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

        Assert.assertEquals(ClickHouseByteBuffer
                .of(Arrays.asList(new byte[0], new byte[] { 0x1 }, new byte[0],
                        new byte[] { 0x2, 0x3 }), 0, 2),
                ClickHouseByteBuffer.of(new byte[] { 0x1, 0x2 }, 0, 2));
        Assert.assertEquals(ClickHouseByteBuffer
                .of(Arrays.asList(new byte[0], new byte[] { 0x1 }, new byte[0],
                        new byte[] { 0x2, 0x3 }), 1, 2),
                ClickHouseByteBuffer.of(new byte[] { 0x2, 0x3 }, 0, 2));
    }

    @Test(groups = { "unit" })
    public void testReverse() {
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().reverse(), ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().update(new byte[] { 1 }).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 1 }));
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().update(new byte[] { 1, 2 }).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 2, 1 }));
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().update(new byte[] { 1, 2, 3 }).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 3, 2, 1 }));

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 0).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 0));
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 1).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 1));
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5 }, 1, 2).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 1, 3, 2, 4, 5 }, 1, 2));
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5 }, 1, 3).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 1, 4, 3, 2, 5 }, 1, 3));
        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5 }, 1, 4).reverse(),
                ClickHouseByteBuffer.of(new byte[] { 1, 5, 4, 3, 2 }, 1, 4));
    }

    @Test(groups = { "unit" })
    public void testSetLength() {
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().length(), 0);
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().setLength(0).length(), 0);
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().setLength(-1).length(), 0);

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3, 4, 5 }, 1, 2).setLength(-1).length(), 0);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        Assert.assertEquals(ClickHouseByteBuffer.of(ByteBuffer.wrap(new byte[] { 1, 2, 3 }, 1, 2)).reset(),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(
                ClickHouseByteBuffer.newInstance()
                        .update(ByteBuffer.wrap(new byte[] { 1, 2, 3 }, 1, 2)),
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2));

        Assert.assertEquals(ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2).reset(),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(ClickHouseByteBuffer.newInstance().update(new byte[] { 1, 2, 3 }, 1, 2),
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2));
    }

    @Test(groups = { "unit" })
    public void testUpdateList() {
        Assert.assertEquals(ClickHouseByteBuffer.of(Arrays.asList(new byte[] { 1, 2, 3 }), 1, 2).reset(),
                ClickHouseByteBuffer.newInstance());
        Assert.assertEquals(
                ClickHouseByteBuffer.newInstance().update(Arrays.asList(new byte[] { 1, 2, 3 }), 1, 2),
                ClickHouseByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2));
        Assert.assertEquals(
                ClickHouseByteBuffer.newInstance()
                        .update(Arrays.asList(new byte[] { 1, 2 }, new byte[] { 3 }), 1, 2),
                ClickHouseByteBuffer.of(new byte[] { 2, 3 }, 0, 2));
    }
}
