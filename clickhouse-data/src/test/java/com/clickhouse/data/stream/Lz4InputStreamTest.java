package com.clickhouse.data.stream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class Lz4InputStreamTest {
    private InputStream generateInputStream(String prefix, int samples, StringBuilder builder) throws IOException {
        builder.setLength(0);

        byte[] result = null;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                Lz4OutputStream lz4Out = new Lz4OutputStream(out, 1024 * 1024, null)) {
            for (int i = 0; i < samples; i++) {
                String s = prefix + i;
                lz4Out.write(s.getBytes(StandardCharsets.UTF_8));
                builder.append(s);
            }
            lz4Out.flush();
            result = out.toByteArray();
            // Assert.assertTrue(result.length < builder.length() / 2);
        }

        return new ByteArrayInputStream(result);
    }

    @DataProvider(name = "samples")
    private Object[][] getSamples() {
        return new Object[][] { { "", 0 }, { "test", 100000 }, { "萌萌哒", 1024 * 1024 },
                { "1😂2萌🥘", 2500000 } };
    };

    @Test(dataProvider = "samples", groups = { "unit" })
    public void testReadByte(String prefix, int samples) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        try (InputStream in = generateInputStream(prefix, samples, builder);
                Lz4InputStream lz4In = new Lz4InputStream(in);
                ByteArrayOutputStream out = new ByteArrayOutputStream();) {
            try {
                while (true) {
                    out.write(0xFF & lz4In.readByte());
                }
            } catch (EOFException e) {
                readAll = true;
            }

            out.flush();

            Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), builder.toString());
        }
        Assert.assertTrue(readAll, "All bytes should have read without any issue");
    }

    @Test(dataProvider = "samples", groups = { "unit" })
    public void testReadByteWithAvailable(String prefix, int samples) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        try (InputStream in = generateInputStream(prefix, samples, builder);
                Lz4InputStream lz4In = new Lz4InputStream(in);
                ByteArrayOutputStream out = new ByteArrayOutputStream();) {
            while (true) {
                if (lz4In.available() == 0) {
                    readAll = true;
                    break;
                }

                out.write(0xFF & lz4In.readByte());
            }

            out.flush();

            Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), builder.toString());
        }
        Assert.assertTrue(readAll, "All bytes should have read without any issue");
    }

    @Test(dataProvider = "samples", groups = { "unit" })
    public void testRead(String prefix, int samples) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        try (InputStream in = generateInputStream(prefix, samples, builder);
                Lz4InputStream lz4In = new Lz4InputStream(in);
                ByteArrayOutputStream out = new ByteArrayOutputStream();) {
            int result = 0;
            while ((result = lz4In.read()) != -1) {
                out.write(result);
            }
            out.flush();
            readAll = true;

            Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), builder.toString());
        }
        Assert.assertTrue(readAll, "All bytes should have read without any issue");
    }

    @Test(dataProvider = "samples", groups = { "unit" })
    public void testReadWithAvailable(String prefix, int samples) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        try (InputStream in = generateInputStream(prefix, samples, builder);
                Lz4InputStream lz4In = new Lz4InputStream(in);
                ByteArrayOutputStream out = new ByteArrayOutputStream();) {
            while (lz4In.available() > 0) {
                out.write(lz4In.read());
            }
            out.flush();
            readAll = true;

            Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), builder.toString());
        }
        Assert.assertTrue(readAll, "All bytes should have read without any issue");
    }

    @Test(groups = { "unit" })
    public void testTest() throws IOException {
        testReadBytes("test", 100000);
    }

    @Test(dataProvider = "samples", groups = { "unit" })
    public void testReadBytes(String prefix, int samples) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        for (int i : new int[] { 1, 2, 3, 11, 1025 }) {
            byte[] bytes = new byte[i];
            try (InputStream in = generateInputStream(prefix, samples, builder);
                    Lz4InputStream lz4In = new Lz4InputStream(in);
                    ByteArrayOutputStream out = new ByteArrayOutputStream();) {
                int result = 0;
                while ((result = lz4In.read(bytes)) != -1) {
                    out.write(bytes, 0, result);
                }
                out.flush();
                readAll = true;

                Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), builder.toString());
            }
            Assert.assertTrue(readAll, "All bytes should have read without any issue");
        }
    }

    @Test(dataProvider = "samples", groups = { "unit" })
    public void testReadBytesWithAvailable(String prefix, int samples) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        for (int i : new int[] { 1, 2, 3, 11, 1025 }) {
            byte[] bytes = new byte[i];
            try (InputStream in = generateInputStream(prefix, samples, builder);
                    Lz4InputStream lz4In = new Lz4InputStream(in);
                    ByteArrayOutputStream out = new ByteArrayOutputStream();) {
                while (lz4In.available() > 0) {
                    int result = lz4In.read(bytes);
                    out.write(bytes, 0, result);
                }
                out.flush();
                readAll = true;

                Assert.assertEquals(new String(out.toByteArray(), StandardCharsets.UTF_8), builder.toString());
            }
            Assert.assertTrue(readAll, "All bytes should have read without any issue");
        }
    }

    @Test(groups = { "unit" })
    public void testLZ4Stream() throws IOException {
        StringBuilder sb = new StringBuilder();
        byte[] result = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Lz4OutputStream outputStream = new Lz4OutputStream(baos, 1024 * 1024, null)) {
            for (int i = 0; i < 100000; i++) {
                outputStream.write(("test" + i).getBytes(StandardCharsets.US_ASCII));
                sb.append("test").append(i);
            }
            outputStream.flush();
            result = baos.toByteArray();
            // System.out.println(result.length);
            Assert.assertTrue(result.length < sb.length() / 2);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(result);
                Lz4InputStream is = new Lz4InputStream(bais)) {
            byte[] buf = new byte[20000000];
            int read = is.read(buf);
            // System.out.println(read);
            Assert.assertEquals(new String(buf, 0, read), sb.toString());
        }
    }
}
