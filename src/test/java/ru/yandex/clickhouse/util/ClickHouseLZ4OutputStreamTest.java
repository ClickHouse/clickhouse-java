package ru.yandex.clickhouse.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseLZ4OutputStreamTest {
    private byte[] genCompressedByts(int b, int length, int blockSize) throws IOException {
        ByteArrayOutputStream bas = new ByteArrayOutputStream(blockSize * 512);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, blockSize)) {
            for (int i = 0; i < length; i++) {
                out.write(b);
            }
            out.flush();
        }

        byte[] bytes = bas.toByteArray();
        bas.close();
        return bytes;
    }

    @Test
    public void testWrite() throws IOException {
        ByteArrayOutputStream bas = new ByteArrayOutputStream(64);

        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 2)) {
            byte[] bytes = new byte[] { (byte) -36, (byte) -86, (byte) 31, (byte) 113, (byte) -106, (byte) 44,
                    (byte) 99, (byte) 96, (byte) 112, (byte) -7, (byte) 47, (byte) 15, (byte) -63, (byte) 39,
                    (byte) -73, (byte) -104, (byte) -126, (byte) 12, (byte) 0, (byte) 0, (byte) 0, (byte) 2, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 32, (byte) 1, (byte) 2 };
            out.write(1);
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            out.write(2);
            Assert.assertEquals(bas.toByteArray(), bytes);
            out.write(3);
            Assert.assertEquals(bas.toByteArray(), bytes);
            out.flush();
            Assert.assertEquals(bas.toByteArray(),
                    new byte[] { (byte) -36, (byte) -86, (byte) 31, (byte) 113, (byte) -106, (byte) 44, (byte) 99,
                            (byte) 96, (byte) 112, (byte) -7, (byte) 47, (byte) 15, (byte) -63, (byte) 39, (byte) -73,
                            (byte) -104, (byte) -126, (byte) 12, (byte) 0, (byte) 0, (byte) 0, (byte) 2, (byte) 0,
                            (byte) 0, (byte) 0, (byte) 32, (byte) 1, (byte) 2, (byte) 64, (byte) -39, (byte) 21,
                            (byte) 50, (byte) -77, (byte) -124, (byte) 25, (byte) 73, (byte) -59, (byte) 9, (byte) 112,
                            (byte) -38, (byte) 12, (byte) 99, (byte) 71, (byte) 74, (byte) -126, (byte) 11, (byte) 0,
                            (byte) 0, (byte) 0, (byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 16, (byte) 3 });
            bas.close();
        }
    }

    @Test
    public void testWriteBytes() throws IOException {
        Assert.assertThrows(NullPointerException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(new ByteArrayOutputStream(), 3)) {
                    out.write(null);
                }
            }
        });

        ByteArrayOutputStream bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            out.write(new byte[0]);
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            
            byte[] bytes = new byte[] { (byte) 13, (byte) 13 };
            out.write(bytes);
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 2, 3));
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            byte[] expected = genCompressedByts(13, 3, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 3, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 4, 3));
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            byte[] expected = genCompressedByts(13, 6, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 6, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 7, 3));
            bas.close();
        }
    }

    @Test
    public void testWriteBytesWithOffset() throws IOException {
        Assert.assertThrows(NullPointerException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(new ByteArrayOutputStream(), 3)) {
                    out.write(null, 0, 1);
                }
            }
        });
        Assert.assertThrows(IndexOutOfBoundsException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(new ByteArrayOutputStream(), 3)) {
                    out.write(new byte[0], 0, 1);
                }
            }
        });
        Assert.assertThrows(IndexOutOfBoundsException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(new ByteArrayOutputStream(), 3)) {
                    out.write(new byte[0], -1, 0);
                }
            }
        });
        Assert.assertThrows(IndexOutOfBoundsException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(new ByteArrayOutputStream(), 3)) {
                    out.write(new byte[1], 1, 1);
                }
            }
        });

        final byte[] bytes = new byte[] { (byte) 0, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13,
                (byte) 13, (byte) 0 };
        ByteArrayOutputStream bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            out.write(bytes, 1, 0);
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            out.write(bytes, 1, 2);
            Assert.assertEquals(bas.toByteArray(), new byte[0]);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 2, 3));
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            out.write(bytes, 1, 3);
            byte[] expected = genCompressedByts(13, 3, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            out.write(bytes, 1, 4);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 3, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 4, 3));
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            out.write(bytes, 1, 6);
            byte[] expected = genCompressedByts(13, 6, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (ClickHouseLZ4OutputStream out = new ClickHouseLZ4OutputStream(bas, 3)) {
            out.write(bytes, 1, 7);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 6, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 7, 3));
            bas.close();
        }
    }
}
