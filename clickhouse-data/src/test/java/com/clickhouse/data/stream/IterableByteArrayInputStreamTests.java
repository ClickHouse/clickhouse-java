package com.clickhouse.data.stream;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.Queue;

public class IterableByteArrayInputStreamTests {

    @Test(groups = { "unit" })
    public void testIterableByteArrayInputStream() throws Exception {
        LinkedList<byte[]> buffers = new LinkedList<>();
        buffers.add("Hello".getBytes());
        buffers.add("World".getBytes());
        IterableByteArrayInputStream is = new IterableByteArrayInputStream(buffers, null);
        byte[] buffer = new byte[5];
        is.read(buffer, 0, 5);
        Assert.assertEquals(new String(buffer), "Hello");
        is.read(buffer, 0, 5);
        Assert.assertEquals(new String(buffer) ,"World");
        is.close();
    }

    @Test(groups = { "unit" })
    public void testByteArrayQueueInputStream() throws Exception {
        Queue<byte[]> buffers = new LinkedList<>();
        buffers.add("Hello".getBytes());
        buffers.add("World".getBytes());
        ByteArrayQueueInputStream is = new ByteArrayQueueInputStream(buffers, null);
        byte[] buffer = new byte[5];
        is.read(buffer, 0, 5);
        Assert.assertEquals(new String(buffer), "Hello");
        is.read(buffer, 0, 5);
        Assert.assertEquals(new String(buffer) ,"World");
        Assert.assertTrue(buffers.isEmpty());
        is.close();
    }
}
