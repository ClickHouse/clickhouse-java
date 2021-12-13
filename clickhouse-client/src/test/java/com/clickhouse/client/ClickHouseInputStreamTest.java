package com.clickhouse.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseInputStreamTest {
    private InputStream generateInputStream(byte[] bytes) {
        if (bytes.length > 0) {
            new Random().nextBytes(bytes);
        }
        return new BufferedInputStream(new ByteArrayInputStream(bytes));
    }

    @Test(groups = { "unit" })
    public void testNullEmptyOrClosedInput() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseInputStream.of(null));
        ClickHouseInputStream empty = ClickHouseInputStream
                .of(generateInputStream(new byte[0]));
        Assert.assertEquals(empty.isClosed(), false);
        Assert.assertEquals(empty.available(), 0);
        Assert.assertEquals(empty.read(), -1);
        Assert.assertEquals(empty.read(), -1);
        Assert.assertEquals(empty.read(new byte[1]), -1);
        Assert.assertEquals(empty.read(new byte[1]), -1);
        Assert.assertEquals(empty.readBytes(0), new byte[0]);
        Assert.assertThrows(EOFException.class, () -> empty.readByte());
        Assert.assertEquals(empty.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty.read());

        ClickHouseInputStream empty1 = ClickHouseInputStream
                .of(generateInputStream(new byte[0]));
        Assert.assertEquals(empty1.isClosed(), false);
        Assert.assertThrows(EOFException.class, () -> empty1.readBytes(1));
        Assert.assertEquals(empty1.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty1.read());

        InputStream in = generateInputStream(new byte[] { (byte) 123 });
        in.close();
        ClickHouseInputStream chIn = ClickHouseInputStream.of(in);
        Assert.assertEquals(chIn.isClosed(), false);
        Assert.assertThrows(IOException.class, () -> chIn.available());
        Assert.assertEquals(chIn.isClosed(), false);
        Assert.assertEquals(ClickHouseInputStream.of(chIn), chIn);
        Assert.assertEquals(chIn.readBytes(0), new byte[0]);
        Assert.assertThrows(IOException.class, () -> chIn.readBytes(1));
        Assert.assertThrows(IOException.class, () -> chIn.read());
        Assert.assertThrows(IOException.class, () -> chIn.readByte());
        Assert.assertThrows(IOException.class, () -> chIn.read(new byte[0]));
        chIn.close();
        Assert.assertEquals(chIn.isClosed(), true);
    }

    @Test(groups = { "unit" })
    public void testWrappedInput() throws IOException {
        int sample = 10000;
        byte[] bytes = new byte[sample];
        try (InputStream in = generateInputStream(bytes); ClickHouseInputStream chIn = ClickHouseInputStream.of(in)) {
            for (int i = 0; i < sample; i++) {
                Assert.assertTrue(chIn.available() > 0);
                Assert.assertEquals(chIn.readByte(), bytes[i]);
            }

            Assert.assertEquals(chIn.available(), 0);
            Assert.assertFalse(chIn.isClosed(), "Should not be closed");

            Assert.assertThrows(EOFException.class, () -> chIn.readByte());
            Assert.assertTrue(chIn.isClosed(), "Should have been closed automatically");
        }

        try (InputStream in = generateInputStream(bytes); ClickHouseInputStream chIn = ClickHouseInputStream.of(in)) {
            Assert.assertEquals(chIn.readBytes(sample), bytes);
            Assert.assertFalse(chIn.isClosed(), "Should not be closed");
            Assert.assertThrows(EOFException.class, () -> chIn.readBytes(1));
            Assert.assertTrue(chIn.isClosed(), "Should have been closed automatically");
        }
    }

    @Test(groups = { "unit" })
    public void testNullOrEmptyBlockingInput() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseInputStream.of(null, 0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseInputStream.of(new ArrayBlockingQueue<>(0), -1));

        BlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<>(1);
        ClickHouseInputStream empty = ClickHouseInputStream.of(queue, 10);
        Assert.assertEquals(empty.isClosed(), false);
        Assert.assertThrows(IOException.class, () -> empty.available());
        Assert.assertThrows(IOException.class, () -> empty.read());
        Assert.assertThrows(IOException.class, () -> empty.read(new byte[1]));
        Assert.assertEquals(empty.readBytes(0), new byte[0]);
        Assert.assertThrows(IOException.class, () -> empty.readByte());
        Assert.assertThrows(IOException.class, () -> empty.readBytes(1));
        Assert.assertEquals(empty.isClosed(), false);

        queue.offer(ClickHouseInputStream.EMPTY_BUFFER);
        Assert.assertEquals(empty.available(), 0);
        Assert.assertEquals(empty.read(), -1);
        Assert.assertEquals(empty.read(), -1);
        Assert.assertEquals(empty.read(new byte[1]), -1);
        Assert.assertEquals(empty.read(new byte[2]), -1);
        Assert.assertThrows(EOFException.class, () -> empty.readByte());
        Assert.assertEquals(empty.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty.read());
    }

    @Test(groups = { "unit" })
    public void testBlockingInput() throws IOException {
        BlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<>();
        Random r = new Random();
        byte[] values = new byte[1234567];
        r.nextBytes(values);
        for (int i = 0; i < values.length; i++) {
            int len = values.length - i - 1;
            if (len > 1024) {
                len = r.nextInt(1024);
            }
            byte[] bytes = new byte[len + 1];
            System.arraycopy(values, i, bytes, 0, bytes.length);
            queue.offer(ByteBuffer.wrap(bytes));
            i += bytes.length - 1;
        }
        queue.offer(ClickHouseInputStream.EMPTY_BUFFER);

        ClickHouseInputStream in = ClickHouseInputStream.of(queue, 100);
        for (int i = 0; i < values.length; i++) {
            int length = Math.min(2048, values.length - i - 1) + 1;
            Assert.assertTrue(in.available() > 0, "Should have at least " + length + " byte(s) to read");
            Assert.assertEquals(in.readBytes(length), Arrays.copyOfRange(values, i, i + length));
            i += length - 1;
        }
        Assert.assertFalse(in.isClosed(), "Should not be closed");
        Assert.assertTrue(in.available() == 0, "Should have all bytes read");
        in.close();
        Assert.assertTrue(in.available() == 0, "Should have all bytes read");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }

    @Test(groups = { "unit" })
    public void testBlockingInputAsync() throws IOException {
        BlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<>();
        Random r = new Random();
        byte[] values = new byte[1234567];
        r.nextBytes(values);

        new Thread(() -> {
            for (int i = 0; i < values.length; i++) {
                int len = values.length - i - 1;
                if (len > 1024) {
                    len = r.nextInt(1024);
                }
                byte[] bytes = new byte[len + 1];
                System.arraycopy(values, i, bytes, 0, bytes.length);
                queue.offer(ByteBuffer.wrap(bytes));
                i += bytes.length - 1;
            }
            queue.offer(ClickHouseInputStream.EMPTY_BUFFER);
        }).start();
        ClickHouseInputStream in = ClickHouseInputStream.of(queue, 0);
        for (int i = 0; i < values.length; i++) {
            int length = Math.min(2048, values.length - i - 1) + 1;
            Assert.assertTrue(in.available() > 0, "Should have at least " + length + " byte(s) to read");
            Assert.assertEquals(in.readBytes(length), Arrays.copyOfRange(values, i, i + length));
            i += length - 1;
        }
        Assert.assertFalse(in.isClosed(), "Should not be closed");
        Assert.assertTrue(in.available() == 0, "Should have all bytes read");
        in.close();
        Assert.assertTrue(in.available() == 0, "Should have all bytes read");
        Assert.assertTrue(in.isClosed(), "Should have been closed");
    }
}
