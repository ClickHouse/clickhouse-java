package com.clickhouse.client.data;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ClickhouseLZ4InputStreamTest {
    private InputStream generateInputStream(String prefix, int samples, StringBuilder builder) throws IOException {
        builder.setLength(0);

        byte[] result = null;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ClickHouseLZ4OutputStream lz4Out = new ClickHouseLZ4OutputStream(out, 1024 * 1024)) {
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

    @DataProvider(name = "prefixes")
    private Object[][] getPrefixes() {
        return new Object[][] { { "test" }, { "萌萌哒" },
                { "1😂2萌🥘" } };
    };

    @Test(dataProvider = "prefixes", groups = { "unit" })
    public void testReadByte(String prefix) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        try (InputStream in = generateInputStream(prefix, 10000, builder);
                ClickHouseLZ4InputStream lz4In = new ClickHouseLZ4InputStream(in);
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

    @Test(dataProvider = "prefixes", groups = { "unit" })
    public void testRead(String prefix) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        try (InputStream in = generateInputStream(prefix, 10000, builder);
                ClickHouseLZ4InputStream lz4In = new ClickHouseLZ4InputStream(in);
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

    @Test(dataProvider = "prefixes", groups = { "unit" })
    public void testReadBytes(String prefix) throws IOException {
        StringBuilder builder = new StringBuilder();
        boolean readAll = false;
        for (int i = 1; i < 1025; i++) {
            byte[] bytes = new byte[i];
            try (InputStream in = generateInputStream(prefix, 10000, builder);
                    ClickHouseLZ4InputStream lz4In = new ClickHouseLZ4InputStream(in);
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

    @Test(groups = { "unit" })
    public void testLZ4Stream() throws IOException {
        StringBuilder sb = new StringBuilder();
        byte[] result = null;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ClickHouseLZ4OutputStream outputStream = new ClickHouseLZ4OutputStream(baos, 1024 * 1024)) {
            for (int i = 0; i < 100000; i++) {
                outputStream.write(("test" + i).getBytes());
                sb.append("test").append(i);
            }
            outputStream.flush();
            result = baos.toByteArray();
            // System.out.println(result.length);
            Assert.assertTrue(result.length < sb.length() / 2);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(result);
                ClickHouseLZ4InputStream is = new ClickHouseLZ4InputStream(bais)) {
            byte[] buf = new byte[20000000];
            int read = is.read(buf);
            // System.out.println(read);
            Assert.assertEquals(new String(buf, 0, read), sb.toString());
        }
    }
}
