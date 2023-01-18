package com.clickhouse.data.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.clickhouse.data.format.BinaryStreamUtilsTest;

public class Lz4OutputStreamTest {
    private byte[] genCompressedByts(int b, int length, int blockSize) throws IOException {
        ByteArrayOutputStream bas = new ByteArrayOutputStream(blockSize * 512);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, blockSize, null)) {
            for (int i = 0; i < length; i++) {
                out.write(b);
            }
            out.flush();
        }

        byte[] bytes = bas.toByteArray();
        bas.close();
        return bytes;
    }

    @Test(groups = { "unit" })
    public void testCompressAndDecompressQuery() throws IOException {
        ByteArrayOutputStream bas = new ByteArrayOutputStream(100);
        String sql = "select '4d67f5c7-60ae-4a00-8ed1-701429fa2cdf'";
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 1024 * 1024, null)) {
            out.write(sql.getBytes(StandardCharsets.UTF_8));
            out.flush();
        }

        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(
                0xD2, 0x73, 0x99, 0x63, 0xA3, 0xD0, 0xFB, 0xAD, 0x31, 0xE1, 0x9F, 0xC1, 0x5A, 0x2B, 0xB2, 0x8B,
                0x82, 0x38, 0, 0, 0, 0x2D, 0, 0, 0, 0xF0, 0x1E, 0x73, 0x65, 0x6C, 0x65, 0x63,
                0x74, 0x20, 0x27, 0x34, 0x64, 0x36, 0x37, 0x66, 0x35, 0x63, 0x37, 0x2D, 0x36, 0x30, 0x61, 0x65,
                0x2D, 0x34, 0x61, 0x30, 0x30, 0x2D, 0x38, 0x65, 0x64, 0x31, 0x2D, 0x37, 0x30, 0x31, 0x34, 0x32,
                0x39, 0x66, 0x61, 0x32, 0x63, 0x64, 0x66, 0x27));

        try (Lz4InputStream in = new Lz4InputStream(new ByteArrayInputStream(bas.toByteArray()))) {
            byte[] bytes = new byte[1024];
            int len = in.read(bytes);
            String s = new String(bytes, 0, len, StandardCharsets.UTF_8);
            Assert.assertEquals(s, sql);
        }
    }

    @Test(groups = { "unit" })
    public void testWrite() throws IOException {
        ByteArrayOutputStream bas = new ByteArrayOutputStream(64);

        try (Lz4OutputStream out = new Lz4OutputStream(bas, 2, null)) {
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

    @Test(groups = { "unit" })
    public void testWriteBytes() throws IOException {
        Assert.assertThrows(NullPointerException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (Lz4OutputStream out = new Lz4OutputStream(new ByteArrayOutputStream(), 3, null)) {
                    out.write(null);
                }
            }
        });

        ByteArrayOutputStream bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
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
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            byte[] expected = genCompressedByts(13, 3, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 3, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 4, 3));
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            byte[] expected = genCompressedByts(13, 6, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            byte[] bytes = new byte[] { (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13 };
            out.write(bytes);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 6, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 7, 3));
            bas.close();
        }
    }

    @Test(groups = { "unit" })
    public void testWriteBytesWithOffset() throws IOException {
        Assert.assertThrows(NullPointerException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (Lz4OutputStream out = new Lz4OutputStream(new ByteArrayOutputStream(), 3, null)) {
                    out.write(null, 0, 1);
                }
            }
        });
        Assert.assertThrows(IndexOutOfBoundsException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (Lz4OutputStream out = new Lz4OutputStream(new ByteArrayOutputStream(), 3, null)) {
                    out.write(new byte[0], 0, 1);
                }
            }
        });
        Assert.assertThrows(IndexOutOfBoundsException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (Lz4OutputStream out = new Lz4OutputStream(new ByteArrayOutputStream(), 3, null)) {
                    out.write(new byte[0], -1, 0);
                }
            }
        });
        Assert.assertThrows(IndexOutOfBoundsException.class, new Assert.ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                try (Lz4OutputStream out = new Lz4OutputStream(new ByteArrayOutputStream(), 3, null)) {
                    out.write(new byte[1], 1, 1);
                }
            }
        });

        final byte[] bytes = new byte[] { (byte) 0, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13, (byte) 13,
                (byte) 13, (byte) 0 };
        ByteArrayOutputStream bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
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
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            out.write(bytes, 1, 3);
            byte[] expected = genCompressedByts(13, 3, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            out.write(bytes, 1, 4);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 3, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 4, 3));
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            out.write(bytes, 1, 6);
            byte[] expected = genCompressedByts(13, 6, 3);
            Assert.assertEquals(bas.toByteArray(), expected);
            out.flush();
            Assert.assertEquals(bas.toByteArray(), expected);
            bas.close();
        }

        bas = new ByteArrayOutputStream(64);
        try (Lz4OutputStream out = new Lz4OutputStream(bas, 3, null)) {
            out.write(bytes, 1, 7);
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 6, 3));
            out.flush();
            Assert.assertEquals(bas.toByteArray(), genCompressedByts(13, 7, 3));
            bas.close();
        }
    }
}
