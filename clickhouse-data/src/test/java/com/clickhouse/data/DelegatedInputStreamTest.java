package com.clickhouse.data;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DelegatedInputStreamTest {
    @Test(groups = { "unit" })
    public void testRead() throws IOException {
        try (DelegatedInputStream in = new DelegatedInputStream(null, w -> {
            for (int i = 0; i < Byte.MAX_VALUE; i++) {
                w.write(i);
            }
        })) {
            Assert.assertFalse(in.isClosed());
            Assert.assertTrue(in.available() > 0);
            for (int i = 0; i < Byte.MAX_VALUE; i++) {
                Assert.assertEquals(in.read(), i);
            }
            Assert.assertEquals(in.read(), -1);
            Assert.assertEquals(in.read(), -1);
            Assert.assertFalse(in.isClosed());
            in.close();
            Assert.assertTrue(in.isClosed());
            Assert.assertEquals(in.available(), 0);
        }
    }

    @Test(groups = { "unit" })
    public void testReadBytes() throws IOException {
        try (DelegatedInputStream in = new DelegatedInputStream(w -> {
            for (int i = 0; i < Byte.MAX_VALUE; i++) {
                w.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6 });
            }
        })) {
            Assert.assertFalse(in.isClosed());
            Assert.assertTrue(in.available() > 0);
            for (int i = 0; i < Byte.MAX_VALUE; i++) {
                Assert.assertEquals(in.readBytes(6), new byte[] { 1, 2, 3, 4, 5, 6 });
            }
            Assert.assertEquals(in.read(), -1);
            Assert.assertFalse(in.isClosed());
            in.close();
            Assert.assertTrue(in.isClosed());
            Assert.assertEquals(in.available(), 0);
        }
    }
}
