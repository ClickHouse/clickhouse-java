package com.clickhouse.client.http;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

public class ByteArraysInputStreamTest {

    @Test(groups = { "unit" })
    public void testInvalidValue() {
        Assert.assertThrows(NullPointerException.class,
                () -> ByteArraysInputStream.of((List<byte[]>) null));
        Assert.assertThrows(NullPointerException.class,
                () -> ByteArraysInputStream.of(new byte[] {1}, null));
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        ByteArraysInputStream in1 = ByteArraysInputStream.of(new byte[] {1,2,3});
        Assert.assertEquals(in1.pos, 0);
        Assert.assertEquals(in1.count, 3);
        Assert.assertEquals(in1.mark, 0);
        Assert.assertEquals(in1.current, 0);
        Assert.assertEquals(in1.currentPos, 0);

        ByteArraysInputStream in2 = ByteArraysInputStream.of(new byte[] {1,2,3}, new byte[] {4,5,6});
        Assert.assertEquals(in2.pos, 0);
        Assert.assertEquals(in2.count, 6);
        Assert.assertEquals(in2.mark, 0);
        Assert.assertEquals(in2.current, 0);
        Assert.assertEquals(in2.currentPos, 0);
    }

    @DataProvider(name = "getIn")
    protected Object[][] getIn() {
        return new Object[][] {
                { ByteArraysInputStream.of(new byte[] {}) },
                { ByteArraysInputStream.of(new byte[] {1,2,3}) },
                { ByteArraysInputStream.of(new byte[] {1,2,3}, new byte[] {4,5,6}) },
                { ByteArraysInputStream.of(new byte[] {1,2,3}, new byte[0], new byte[] {4,5,6}) },
        };
    }

    @Test(dataProvider = "getIn", groups = { "unit" })
    public void testRead(ByteArraysInputStream in) {
        for (int i=0; i<in.count; i++) {
            Assert.assertEquals(in.read(), i+1);
        }
        Assert.assertEquals(in.read(), -1);
    }

    @Test(dataProvider = "getIn", groups = { "unit" })
    public void testReadArray(ByteArraysInputStream in) {
        byte[] buf = new byte[10];

        // readLen = 2
        int readLen = 2;
        int totalRead = 0;
        while (totalRead < in.count && totalRead + readLen <= in.count) {
            int read = in.read(buf, totalRead, readLen);
            Assert.assertEquals(read, readLen);
            Assert.assertEquals(buf[totalRead], totalRead + 1);
            Assert.assertEquals(buf[totalRead + 1], totalRead + 2);
            totalRead += readLen;
        }

        if (in.count % readLen == 0) {
            Assert.assertEquals(totalRead, in.count);
            int read = in.read(buf, totalRead, readLen);
            Assert.assertEquals(read, -1);
            Assert.assertEquals(buf[totalRead], 0);
        } else {
            // remains 1 byte in `in`
            Assert.assertEquals(totalRead + 1, in.count);
            int read = in.read(buf, totalRead, readLen);
            Assert.assertEquals(read, 1);
            Assert.assertEquals(buf[totalRead], totalRead + 1);

            // read nothing
            read = in.read(buf, totalRead, readLen);
            Assert.assertEquals(read, -1);
        }
    }

    @Test(dataProvider = "getIn", groups = { "unit" })
    public void testReadArrayWithBoundary(ByteArraysInputStream in) {
        byte[] buf = new byte[10];

        // readLen = 3
        int readLen = 3;
        int totalRead = 0;
        while (totalRead<in.count && totalRead + readLen <= in.count) {
            int read = in.read(buf, totalRead, readLen);
            Assert.assertEquals(read, readLen);
            Assert.assertEquals(buf[totalRead], totalRead + 1);
            Assert.assertEquals(buf[totalRead+1], totalRead + 2);
            Assert.assertEquals(buf[totalRead+2], totalRead + 3);
            totalRead += readLen;
        }

        // remain nothing in `in`
        Assert.assertEquals(in.count % readLen, 0);
        Assert.assertEquals(totalRead, in.count);
        int read = in.read(buf, totalRead, readLen);
        Assert.assertEquals(read, -1);
        Assert.assertEquals(buf[totalRead], 0);
    }


    @Test(groups = { "unit" })
    public void testSkip() {
        ByteArraysInputStream in = ByteArraysInputStream.of(new byte[] {1,2,3}, new byte[0], new byte[] {4,5,6});

        int skipped = (int) in.skip(1);
        Assert.assertEquals(skipped, 1);
        Assert.assertEquals(in.pos, 1);
        Assert.assertEquals(in.current, 0);
        Assert.assertEquals(in.currentPos, 1);

        skipped = (int) in.skip(2);
        Assert.assertEquals(skipped, 2);
        Assert.assertEquals(in.pos, 3);
        Assert.assertEquals(in.current, 0);
        Assert.assertEquals(in.currentPos, 3);

        skipped = (int) in.skip(1);
        Assert.assertEquals(skipped, 1);
        Assert.assertEquals(in.pos, 4);
        Assert.assertEquals(in.current, 2);
        Assert.assertEquals(in.currentPos, 1);

        skipped = (int) in.skip(2);
        Assert.assertEquals(skipped, 2);
        Assert.assertEquals(in.pos, 6);
        Assert.assertEquals(in.current, 2);
        Assert.assertEquals(in.currentPos, 3);

        skipped = (int) in.skip(1);
        Assert.assertEquals(skipped, 0);
        Assert.assertEquals(in.pos, 6);
        Assert.assertEquals(in.current, 2);
        Assert.assertEquals(in.currentPos, 3);

        ByteArraysInputStream in2 = ByteArraysInputStream.of(new byte[] {1,2,3}, new byte[0], new byte[] {4,5,6});

        skipped = (int) in2.skip(10);
        Assert.assertEquals(skipped, 6);
        Assert.assertEquals(in2.pos, 6);
        Assert.assertEquals(in2.current, 2);
        Assert.assertEquals(in2.currentPos, 3);
    }

    @Test(groups = { "unit" })
    public void testMark() {
        ByteArraysInputStream in = ByteArraysInputStream.of(new byte[] {1,2,3}, new byte[0], new byte[] {4,5,6});

        in.skip(1);
        in.mark(0);
        Assert.assertEquals(in.mark, 1);

        in.skip(1);
        in.reset();
        Assert.assertEquals(in.pos, 1);
        Assert.assertEquals(in.current, 0);
        Assert.assertEquals(in.currentPos, 1);

        in.skip(2);
        in.mark(0);
        Assert.assertEquals(in.mark, 3);
        Assert.assertEquals(in.pos, 3);
        Assert.assertEquals(in.current, 0);
        Assert.assertEquals(in.currentPos, 3);
    }

}
