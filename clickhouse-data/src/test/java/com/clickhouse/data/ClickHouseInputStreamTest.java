package com.clickhouse.data;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        Assert.assertEquals(chIn.read(new byte[0]), 0);
        chIn.close();
        Assert.assertEquals(chIn.isClosed(), true);
    }

    @Test(groups = { "unit" })
    public void testWrappedInput() throws IOException {
        int sample = 10000;
        byte[] bytes = new byte[sample];
        try (InputStream in = generateInputStream(bytes);
                ClickHouseInputStream chIn = ClickHouseInputStream.of(in)) {
            for (int i = 0; i < sample; i++) {
                Assert.assertTrue(chIn.available() > 0);
                Assert.assertEquals(chIn.readByte(), bytes[i]);
            }

            Assert.assertEquals(chIn.available(), 0);
            Assert.assertFalse(chIn.isClosed(), "Should not be closed");

            Assert.assertThrows(EOFException.class, () -> chIn.readByte());
            Assert.assertTrue(chIn.isClosed(), "Should have been closed automatically");
        }

        try (InputStream in = generateInputStream(bytes);
                ClickHouseInputStream chIn = ClickHouseInputStream.of(in)) {
            Assert.assertEquals(chIn.readBytes(sample), bytes);
            Assert.assertFalse(chIn.isClosed(), "Should not be closed");
            Assert.assertThrows(EOFException.class, () -> chIn.readBytes(1));
            Assert.assertTrue(chIn.isClosed(), "Should have been closed automatically");
        }
    }

    @Test(groups = { "unit" })
    public void testNullOrEmptyBlockingInput() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseInputStream.of((BlockingQueue<ByteBuffer>) null, 0));
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

        queue.offer(ClickHouseByteBuffer.EMPTY_BUFFER);
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
        queue.offer(ClickHouseByteBuffer.EMPTY_BUFFER);

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
            queue.offer(ClickHouseByteBuffer.EMPTY_BUFFER);
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

    @Test(groups = { "unit" })
    public void testPostCloseAction() throws IOException {
        try (ClickHouseInputStream in = ClickHouseInputStream
                .of(generateInputStream("abc".getBytes(StandardCharsets.US_ASCII)), 0, () -> {
                    throw new UncheckedIOException(new IOException("fake exception"));
                })) {
            Assert.assertThrows(IOException.class, () -> in.close());
        }
    }

    @Test(groups = { "unit" })
    public void testReadVarInt() throws IOException {
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { 0x00 }).readVarInt(), 0);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { 0x01 }).readVarInt(), 1);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { 0x02 }).readVarInt(), 2);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { (byte) 0x7F }).readVarInt(), 127);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { (byte) 0x80, 0x01 }).readVarInt(), 128);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { (byte) 0xFF, 0x01 }).readVarInt(), 255);
        Assert.assertEquals(
                ClickHouseInputStream.of(new byte[] { (byte) 0xDD, (byte) 0xC7, 0x01 }).readVarInt(),
                25565);
        Assert.assertEquals(
                ClickHouseInputStream.of(new byte[] { (byte) 0xFF, (byte) 0xFF, 0x7F }).readVarInt(),
                2097151);
        Assert.assertEquals(
                ClickHouseInputStream.of(
                        new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07 })
                        .readVarInt(),
                2147483647);
        Assert.assertEquals(
                ClickHouseInputStream
                        .of(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                0x07, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F })
                        .readVarInt(),
                2147483647);
        Assert.assertEquals(ClickHouseInputStream
                .of(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F })
                .readVarInt(), -1);
        Assert.assertEquals(
                ClickHouseInputStream
                        .of(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x08 })
                        .readVarInt(),
                -2147483648);
        Assert.assertEquals(
                ClickHouseInputStream
                        .of(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                0x08, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 })
                        .readVarInt(),
                -2147483648);
    }

    @Test(groups = { "unit" })
    public void testReadVarLong() throws IOException {
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { 0x00 }).readVarLong(), 0L);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { 0x01 }).readVarLong(), 1L);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { 0x02 }).readVarLong(), 2L);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { (byte) 0x7F }).readVarLong(), 127L);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { (byte) 0x80, 0x01 }).readVarLong(), 128L);
        Assert.assertEquals(ClickHouseInputStream.of(new byte[] { (byte) 0xFF, 0x01 }).readVarLong(), 255L);
        Assert.assertEquals(
                ClickHouseInputStream.of(new byte[] { (byte) 0xDD, (byte) 0xC7, 0x01 }).readVarLong(),
                25565L);
        Assert.assertEquals(
                ClickHouseInputStream.of(new byte[] { (byte) 0xFF, (byte) 0xFF, 0x7F }).readVarLong(),
                2097151L);
        Assert.assertEquals(
                ClickHouseInputStream.of(
                        new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07 })
                        .readVarLong(),
                2147483647L);
        Assert.assertEquals(
                ClickHouseInputStream
                        .of(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F })
                        .readVarLong(),
                9223372036854775807L);
        Assert.assertEquals(ClickHouseInputStream
                .of(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x01 })
                .readVarLong(), -1L);
        Assert.assertEquals(
                ClickHouseInputStream
                        .of(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                (byte) 0xF8, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x01 })
                        .readVarLong(),
                -2147483648L);
        Assert.assertEquals(
                ClickHouseInputStream
                        .of(new byte[] { (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01 })
                        .readVarLong(),
                -9223372036854775808L);
    }

    @Test(groups = { "unit" })
    public void testSkipInput() throws IOException {
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[0])).skip(0L), 0L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[0])).skip(1L), 0L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[0])).skip(Long.MAX_VALUE),
                0L);

        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[1])).skip(0L), 0L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[1])).skip(1L), 1L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[1])).skip(Long.MAX_VALUE),
                1L);

        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[2])).skip(0L), 0L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[2])).skip(1L), 1L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[2])).skip(Long.MAX_VALUE),
                2L);

        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[5]), 4).skip(0L), 0L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[3]), 2).skip(1L), 1L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[7]), 6).skip(2L), 2L);
        Assert.assertEquals(ClickHouseInputStream.of(generateInputStream(new byte[8]), 6).skip(Long.MAX_VALUE),
                8L);

        ClickHouseInputStream in = ClickHouseInputStream
                .of(new ByteArrayInputStream(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 }), 8);
        Assert.assertEquals(in.read(), 1);
        Assert.assertEquals(in.skip(1L), 1L);
        Assert.assertEquals(in.read(), 3);
        Assert.assertEquals(in.skip(2L), 2L);
        Assert.assertEquals(in.read(), 6);
        Assert.assertEquals(in.skip(3L), 3L);
        Assert.assertEquals(in.read(), 10);
        Assert.assertEquals(in.skip(2L), 1L);
        Assert.assertEquals(in.read(), -1);
    }

    @Test(groups = { "unit" })
    public void testUserData() throws IOException {
        try (ClickHouseInputStream in = ClickHouseInputStream.of("")) {
            Assert.assertEquals(in.userData.size(), 0);
            Assert.assertNull(in.setUserData("context1", 1));
            Assert.assertEquals(in.userData.size(), 1);
            Assert.assertNull(in.setUserData("context2", 2));
            Assert.assertEquals(in.userData.size(), 2);
            Assert.assertNull(in.getUserData("context"));
            Assert.assertEquals(in.getUserData("context", ""), "");
            Assert.assertEquals(in.getUserData("context1"), 1);
            Assert.assertNull(in.removeUserData("context"));
            Assert.assertEquals(in.removeUserData("context1"), 1);
            Assert.assertEquals(in.userData.size(), 1);
            in.close();
            Assert.assertEquals(in.userData.size(), 0);
        }
    }
}
